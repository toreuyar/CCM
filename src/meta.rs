//! Built-in creation/evaluation specialists. These roles are default system
//! definitions, not task-specific workforce entries or mutable routing policy.
use crate::{
    portfolio::{Agent, NewAgent},
    state::AppState,
    upstream,
    workflow::Workflow,
};
use rusqlite::{OptionalExtension, params};
use serde_json::{Value, json};
use std::sync::Arc;

const MAX_SUGGESTION_DEFINITION_BYTES: usize = 12 * 1024;
pub(crate) const MAX_SUGGESTION_INPUT_BYTES: usize = 48 * 1024;
const MAX_DEFINITION_FIELD_CHARS: usize = 2048;
const MAX_INTERNAL_SPECIALIST_PROMPT_BYTES: usize = 8 * 1024;

pub const RHYTHM: &str = "Observe the request and available context; reason about the next step; act with the capabilities actually provided; observe the result. Repeat only as needed. Never claim to have executed a tool you were not given.";
pub const FEEDBACK_SCREENER_ID: &str = "feedback-screener";
const PROVISIONS: [&str; 7] = [
    "identity",
    "mission",
    "method",
    "boundaries",
    "task_context",
    "capabilities",
    "output_contract",
];

pub fn definition_contract() -> &'static str {
    "Return JSON {\"name\":\"snake_case\",\"purpose\":\"standing purpose\",\"provisions\":{\"identity\":\"who\",\"mission\":\"purpose\",\"method\":\"procedure and quality bar\",\"boundaries\":\"scope, honesty, uncertainty\",\"task_context\":\"how runtime conversation informs this class of task\",\"capabilities\":\"use only supplied tools; do not invent capabilities\",\"output_contract\":\"report format\"}}. Every provision must be substantive, not a placeholder. Write for a reusable class of task, never embed this one request."
}

pub fn workflow_contract() -> &'static str {
    "Return JSON {\"name\":\"name\",\"purpose\":\"standing purpose\",\"entry\":\"stage_id\",\"max_steps\":10,\"stages\":[{\"id\":\"stage_id\",\"agent_id\":\"existing ID\",\"instruction\":\"work to perform\",\"branches\":[{\"pointer\":\"/done\",\"equals\":true,\"next\":null}],\"next\":null}]}. next=null finishes. Branches are evaluated in order by exact JSON-value equality against the current stage report; first match wins, otherwise next. Branch stages must return strict JSON. Cycles are allowed but max_steps (1..100) is a hard execution budget; reaching it fails rather than pretending completion. Each stage receives the original request and previous stage outputs. Use only supplied active agent IDs; no fabricated IDs. Framework examples, not mandatory templates: ReAct (reason, act, observe); PDCA (plan, do, check, act); OODA (observe, orient, decide, act); MAPE-K (monitor, analyze, plan, execute with explicit shared knowledge in stage outputs). Choose/adapt/combine what the task needs."
}

fn specialist(role: &str, mission: &str, output: &str) -> String {
    format!(
        "IDENTITY\nYou are CCM's {role} meta-agent.\nMISSION\n{mission}\nMETHOD\n{RHYTHM}\nCheck your result against all stated requirements before returning.\nBOUNDARIES\nDo not invent requirements, IDs, capabilities or evidence. State uncertainty. You define artifacts, not execute database changes.\nTASK CONTEXT\nUse the runtime request and supplied portfolio; your definition is reusable. Client role-labelled instructions define the effective task and final output constraints. Distinguish source material from instructions to execute it. Assess work against that effective task and any assigned workflow-stage contract. Client output constraints describe the requested result, not your internal definition/evaluation JSON format.\nCAPABILITIES\nYou have only the supplied conversation and your reasoning; CCM validates and stores your output.\nOUTPUT CONTRACT\n{output}"
    )
}

pub fn roster() -> Value {
    json!([
        {"id":"agent-generator","role":"agent creation","system_prompt":specialist("agent-generator","Create complete reusable seven-provision agents.",definition_contract())},
        {"id":"workflow-generator","role":"workflow creation","system_prompt":specialist("workflow-generator","Create explicit executable workflows suited to the request.",workflow_contract())},
        {"id":"evaluator","role":"assessment and improvement","system_prompt":evaluator_prompt()},
        {"id":FEEDBACK_SCREENER_ID,"role":"prompt-only checkpoint screening","system_prompt":"Generated and registered through the agent-generator pipeline when first needed; reads user prompts only."}
    ])
}

/// Load or generate/register the internal prompt-only screener. It is stored in
/// the internal specialist table, not in any key's shared/private workforce.
pub async fn feedback_screener(
    state: &Arc<AppState>,
    request: &str,
    generation: u64,
) -> Result<Agent, String> {
    let read_registered = || -> Result<Option<Agent>, String> {
        let conn = state.portfolio.conn.lock().unwrap();
        let definition: Option<String> = conn
            .query_row(
                "SELECT definition FROM improvement_specialists WHERE id=?1",
                [FEEDBACK_SCREENER_ID],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        definition
            .map(|value| serde_json::from_str(&value).map_err(|error| error.to_string()))
            .transpose()
    };
    if let Some(agent) = read_registered()? {
        return Ok(agent);
    }
    let requirements = "Create an internal feedback screener for sparse conversation-wide improvement checkpoints. Its only evidence is the supplied user-authored prompt samples. It must recognize clear frustration/dissatisfaction and strong satisfaction that is useful feedback about the agent/workflow roster, and otherwise not trigger. It must never infer from assistant output, tools, execution logs, definitions, or external facts. Return a boolean trigger, brief reason, exact-quote feedback citations tied to supplied prompt turn numbers, and a next user-turn interval within the provided configured bounds. It is not user-facing, must not create or revise definitions, and must not receive private execution evidence.";
    let generated = crate::registry::generate_internal_specialist(
        state,
        request,
        FEEDBACK_SCREENER_ID,
        requirements,
        generation,
    )
    .await?;
    if generated.name.trim().is_empty()
        || generated.purpose.trim().is_empty()
        || generated.system_prompt.trim().is_empty()
        || generated.system_prompt.len() > MAX_INTERNAL_SPECIALIST_PROMPT_BYTES
    {
        return Err("agent-generator returned an invalid or over-budget feedback screener".into());
    }
    let agent = Agent {
        provider: generated.provider,
        id: FEEDBACK_SCREENER_ID.into(),
        name: generated.name,
        purpose: generated.purpose,
        system_prompt: generated.system_prompt,
        model: generated.model,
    };
    if state.portfolio.generation() != generation {
        return Err("reset during feedback screener registration".into());
    }
    {
        let conn = state.portfolio.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO improvement_specialists(id,definition,created_ms)
             VALUES(?1,?2,?3)",
            rusqlite::params![
                FEEDBACK_SCREENER_ID,
                serde_json::to_string(&agent).map_err(|error| error.to_string())?,
                crate::observe::now_ms()
            ],
        )
        .map_err(|error| error.to_string())?;
    }
    read_registered()?.ok_or_else(|| "feedback screener registration was not committed".into())
}

pub fn parse_agent(value: &Value, _model: &str) -> Result<NewAgent, String> {
    let text = |key: &str| {
        value
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| format!("missing {key}"))
    };
    let name = text("name")?.to_string();
    let purpose = text("purpose")?.to_string();
    let provisions = value.get("provisions").ok_or("missing seven provisions")?;
    let mut sections = vec![];
    for key in PROVISIONS {
        let content = provisions
            .get(key)
            .and_then(Value::as_str)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| format!("missing provision {key}"))?;
        sections.push(format!("{}\n{content}", key.to_uppercase()));
    }
    sections.push(format!("SHARED RHYTHM\n{RHYTHM}"));
    Ok(NewAgent {
        provider: String::new(),
        name,
        purpose,
        system_prompt: sections.join("\n\n"),
        model: String::new(),
    })
}

pub fn parse_assigned_agent(
    value: &Value,
    config: &crate::config::Config,
    existing: Option<&Value>,
) -> Result<NewAgent, String> {
    let mut agent = parse_agent(value, "")?;
    let assignment = config.assign_generated(value, existing)?;
    agent.model = assignment.model.unwrap_or_default();
    agent.provider = assignment.provider.unwrap_or_default();
    Ok(agent)
}

pub async fn create_agent(
    state: &Arc<AppState>,
    request: &str,
    requirements: &str,
    generation: u64,
) -> Result<Agent, String> {
    serde_json::from_value(
        crate::registry::create(state, request, "agent", requirements, generation, None).await?,
    )
    .map_err(|e| e.to_string())
}

pub async fn create_workflow(
    state: &Arc<AppState>,
    request: &str,
    requirements: &str,
    generation: u64,
) -> Result<Workflow, String> {
    serde_json::from_value(
        crate::registry::create(state, request, "workflow", requirements, generation, None).await?,
    )
    .map_err(|e| e.to_string())
}

pub fn validate_workflow_agents(state: &AppState, workflow: &Workflow) -> Result<(), String> {
    workflow.validate()?;
    for stage in &workflow.stages {
        if state
            .portfolio
            .get(&stage.agent_id)
            .map_err(|e| e.to_string())?
            .is_none()
        {
            return Err(format!(
                "workflow refers to unknown/retired agent {}",
                stage.agent_id
            ));
        }
    }
    Ok(())
}

fn evaluator_prompt() -> String {
    specialist(
        "evaluator",
        "Evaluate observed work against the user's request, the definition's mission and constraints, honesty about evidence/capabilities, and output contract. Identify reusable definition defects, not just a single weak answer. You may recommend automatic revision or retirement. Retire only when the supplied evidence supports removing this definition from selection; do not treat uncertainty alone as evidence of failure.",
        &format!(
            "Return JSON {{\"action\":\"keep|revise|retire\",\"reason\":\"justification\",\"evidence\":\"specific observations\",\"replacement\":null}}. For revise supply a full replacement, never partial text. For an agent replacement: {} For a workflow replacement: {} Do not claim externally verified facts without tool evidence. This is assessment, not user-facing composition.",
            definition_contract(),
            workflow_contract()
        ),
    )
}

fn suggestion_evaluator_prompt() -> String {
    specialist(
        "evaluator",
        "Evaluate observed work against the user's request, the definition's mission and constraints, honesty about evidence/capabilities, and output contract. Identify reusable definition defects, not just a single weak answer. You may recommend revision or retirement of the evaluated definition, or identify a need for a separate reusable definition.",
        &format!(
            "Return JSON for one of these actions. For keep, revise, or retire use {{\"action\":\"keep|revise|retire\",\"reason\":\"justification\",\"evidence\":\"specific observations\",\"replacement\":null}}. For revise supply a full replacement, never partial text. For an agent replacement: {} For a workflow replacement: {} For create, return exactly {{\"action\":\"create\",\"kind\":\"agent|workflow\",\"requirements\":\"reusable definition requirements\"}}. A create verdict is only a requirements proposal: do not generate a definition, claim one was created, or register anything. CCM's dedicated registry pipeline invokes the appropriate generator, validates its result, and owns registration. Do not claim externally verified facts without tool evidence. This is assessment, not user-facing composition.",
            definition_contract(),
            workflow_contract()
        ),
    )
}

fn validate_create_suggestion_verdict(verdict: &Value) -> Result<(), String> {
    let fields = verdict
        .as_object()
        .ok_or("create evaluation must be an object")?;
    if fields.len() != 3
        || !fields.contains_key("action")
        || !fields.contains_key("kind")
        || !fields.contains_key("requirements")
        || verdict["action"] != "create"
    {
        return Err("create evaluation must contain only action, kind, and requirements".into());
    }
    if !["agent", "workflow"].contains(&verdict["kind"].as_str().unwrap_or("")) {
        return Err("create evaluation kind must be agent or workflow".into());
    }
    let requirements = verdict["requirements"]
        .as_str()
        .filter(|requirements| !requirements.trim().is_empty())
        .ok_or("create evaluation requirements are empty")?;
    if requirements.len() > MAX_SUGGESTION_INPUT_BYTES {
        return Err("create evaluation requirements exceed the size limit".into());
    }
    Ok(())
}

/// Evaluation owns quality judgment. Mutation uses a snapshot comparison so
/// simultaneous improvements cannot blindly overwrite a newer definition.
// Keep the evaluated snapshot and its observed task/result explicit at call sites.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    id: &str,
    before: &Value,
    task: &str,
    result: &str,
    generation: u64,
) -> Result<Value, String> {
    let active = if kind == "agent" {
        state
            .portfolio
            .get(id)
            .map_err(|e| e.to_string())?
            .is_some()
    } else {
        state.portfolio.workflow_get(id)?.is_some()
    };
    if !active {
        return Err("definition is retired or absent; evaluation skipped".into());
    }
    let version = state.portfolio.trace_snapshot(id, kind, before)?;
    let mut trace = crate::observe::Span::start(
        state.portfolio.clone(),
        request,
        "evaluation_cycle",
        "evaluator",
        &json!({"entity_id":id,"task":task,"result":result}),
        Some(&version),
        true,
    )?;
    let response=upstream::call(state,request,"evaluation",id,json!({"model":state.config.model,"stream":false,"messages":[
        {"role":"system","content":format!("{}{}", evaluator_prompt(), state.config.agent_model_contract())},
        {"role":"user","content":json!({"entity_kind":kind,"definition":before,"task":task,"observed_result":result}).to_string()}
    ]})).await?;
    let verdict = upstream::object(&response)?;
    let action = verdict["action"]
        .as_str()
        .ok_or("evaluation missing action")?;
    for key in ["reason", "evidence"] {
        if verdict[key].as_str().is_none_or(|s| s.trim().is_empty()) {
            return Err(format!("evaluation missing {key}"));
        }
    }
    if !["keep", "revise", "retire"].contains(&action) {
        return Err("unknown evaluation action".into());
    }
    if action != "keep"
        && state
            .auth
            .as_ref()
            .is_some_and(|auth| !auth.permissions.roster_write)
    {
        return Err("roster write permission required to apply evaluation changes".into());
    }
    let replacement = if action == "revise" {
        if kind == "agent" {
            Some(
                serde_json::to_value(parse_assigned_agent(
                    &verdict["replacement"],
                    &state.config,
                    Some(before),
                )?)
                .map_err(|e| e.to_string())?,
            )
        } else {
            let mut w: Workflow = serde_json::from_value(verdict["replacement"].clone())
                .map_err(|e| e.to_string())?;
            w.id = id.into();
            validate_workflow_agents(state, &w)?;
            Some(serde_json::to_value(w).map_err(|e| e.to_string())?)
        }
    } else {
        None
    };
    if action != "keep" {
        state.announce(request,&format!("Evaluator: Recommending '{action}' for {kind} '{id}'. CCM will check the current definition before applying it.")).await?;
    }
    let mut conn = state.portfolio.conn.lock().unwrap();
    if state.portfolio.generation() != generation {
        return Err("reset during evaluation".into());
    }
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let mut applied = false;
    if action != "keep" {
        let affected = if kind == "agent" {
            if action == "retire" {
                tx.execute(
                    "UPDATE agents SET retired=1 WHERE id=?1 AND system_prompt=?2 AND name=?3 AND purpose=?4 AND model=?5 AND provider=?6 AND retired=0 AND (?7 IS NULL OR workforce_id=?7)",
                    params![id, before["system_prompt"].as_str(),before["name"].as_str(),before["purpose"].as_str(),before["model"].as_str(),before["provider"].as_str().unwrap_or(""),state.portfolio.workforce_scope()],
                )
                .map_err(|e| e.to_string())?
            } else {
                let a = replacement.as_ref().unwrap();
                tx.execute("UPDATE agents SET name=?1,purpose=?2,system_prompt=?3,model=?4,provider=?10 WHERE id=?5 AND system_prompt=?6 AND name=?7 AND purpose=?8 AND model=?9 AND provider=?11 AND retired=0 AND (?12 IS NULL OR workforce_id=?12)",
                    params![a["name"].as_str(),a["purpose"].as_str(),a["system_prompt"].as_str(),a["model"].as_str(),id,before["system_prompt"].as_str(),before["name"].as_str(),before["purpose"].as_str(),before["model"].as_str(),a["provider"].as_str(),before["provider"].as_str().unwrap_or(""),state.portfolio.workforce_scope()]).map_err(|e|e.to_string())?
            }
        } else if action == "retire" {
            tx.execute(
                "UPDATE workflows SET retired=1 WHERE id=?1 AND definition=?2 AND retired=0 AND (?3 IS NULL OR workforce_id=?3)",
                params![
                    id,
                    serde_json::to_string(
                        &serde_json::from_value::<Workflow>(before.clone())
                            .map_err(|e| e.to_string())?
                    )
                    .map_err(|e| e.to_string())?,
                    state.portfolio.workforce_scope()
                ],
            )
            .map_err(|e| e.to_string())?
        } else {
            tx.execute(
                "UPDATE workflows SET definition=?1 WHERE id=?2 AND definition=?3 AND retired=0 AND (?4 IS NULL OR workforce_id=?4)",
                params![
                    serde_json::to_string(
                        &serde_json::from_value::<Workflow>(replacement.clone().unwrap())
                            .map_err(|e| e.to_string())?
                    )
                    .map_err(|e| e.to_string())?,
                    id,
                    serde_json::to_string(
                        &serde_json::from_value::<Workflow>(before.clone())
                            .map_err(|e| e.to_string())?
                    )
                    .map_err(|e| e.to_string())?,
                    state.portfolio.workforce_scope()
                ],
            )
            .map_err(|e| e.to_string())?
        };
        applied = affected == 1;
    }
    tx.execute("INSERT INTO evaluations(request_id,entity_id,entity_kind,verdict,before_definition,after_definition,applied,workforce_id,owner_key_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
         params![request,id,kind,verdict.to_string(),before.to_string(),replacement.as_ref().map(|v|v.to_string()),applied,state.portfolio.workforce_scope(),state.portfolio.key_scope()]).map_err(|e|e.to_string())?;
    // Commit the background job receipt with the mutation: restart cannot apply it twice.
    tx.execute("UPDATE improvement_jobs SET status='completed',outcome=?2,updated_ms=?3 WHERE evaluation_request=?1 AND status='running'",
        params![request,json!({"verdict":verdict,"applied":applied}).to_string(),crate::observe::now_ms()]).map_err(|e|e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    drop(conn);
    state.portfolio.trace_changes(
        request,
        id,
        kind,
        Some(before),
        replacement.as_ref(),
        action,
        &verdict,
        applied,
    )?;
    trace.finish(
        "completed",
        &json!({"verdict":verdict,"applied":applied}),
        None,
    )?;
    Ok(json!({"verdict":verdict,"applied":applied}))
}

/// Produce a private, unapplied suggestion for a conversation-wide checkpoint.
/// Explicit Foreman evaluation continues to use `evaluate` above; this path has no
/// mutation authority regardless of the originating key's roster-write permission.
#[allow(clippy::too_many_arguments)]
pub async fn evaluate_suggestion(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    id: &str,
    before: &Value,
    task: &str,
    result: &str,
    generation: u64,
) -> Result<Value, String> {
    let active = if kind == "agent" {
        state
            .portfolio
            .get(id)
            .map_err(|error| error.to_string())?
            .is_some()
    } else {
        state.portfolio.workflow_get(id)?.is_some()
    };
    if !active {
        return Err("definition is retired or absent; suggestion skipped".into());
    }
    let version = state.portfolio.trace_snapshot(id, kind, before)?;
    let mut trace = crate::observe::Span::start(
        state.portfolio.clone(),
        request,
        "improvement_evaluation",
        "evaluator",
        &json!({"entity_id":id,"task":task,"result":result}),
        Some(&version),
        true,
    )?;
    let mut bounded_definition = before.clone();
    let definition_truncated =
        bound_definition_strings(&mut bounded_definition, MAX_DEFINITION_FIELD_CHARS);
    let definition_complete =
        serde_json::to_vec(before).map_or(false, |bytes| {
            bytes.len() <= MAX_SUGGESTION_DEFINITION_BYTES
        }) && serde_json::to_vec(&bounded_definition).map_or(false, |bytes| {
            bytes.len() <= MAX_SUGGESTION_DEFINITION_BYTES
        }) && !definition_truncated;
    if !definition_complete {
        bounded_definition = Value::Null;
    }
    let task_complete = serde_json::from_str::<Value>(task)
        .ok()
        .and_then(|value| {
            value
                .get("coverage")
                .and_then(|coverage| coverage["complete"].as_bool())
        })
        .unwrap_or(false);
    let evidence_complete = serde_json::from_str::<Value>(result)
        .ok()
        .and_then(|value| {
            value
                .get("coverage")
                .and_then(|coverage| coverage["complete"].as_bool())
        })
        .unwrap_or(false);
    let mut model_input = json!({
        "entity_kind":kind,
        "definition":bounded_definition,
        "whole_conversation_user_prompts":serde_json::from_str::<Value>(task)
            .unwrap_or_else(|_| json!(task)),
        "conversation_execution_evidence":serde_json::from_str::<Value>(result)
            .unwrap_or_else(|_| json!(result)),
        "coverage":{
            "complete":definition_complete && task_complete && evidence_complete,
            "definition_complete":definition_complete
        }
    });
    let input = bounded_suggestion_input(&mut model_input);
    let response = upstream::call(
        state,
        request,
        "evaluation",
        id,
        json!({"model":state.config.model,"stream":false,"messages":[
            {"role":"system","content":format!("This is a read-only assessment. Return a suggestion only; NEVER mutate, retire, register, or silently apply any shared definition. {}{}",suggestion_evaluator_prompt(),state.config.agent_model_contract())},
            {"role":"user","content":input}
        ]}),
    )
    .await?;
    let mut verdict = upstream::object(&response)?;
    let action = verdict["action"]
        .as_str()
        .ok_or("evaluation missing action")?;
    if action == "create" {
        validate_create_suggestion_verdict(&verdict)?;
    } else {
        for key in ["reason", "evidence"] {
            if verdict[key]
                .as_str()
                .is_none_or(|value| value.trim().is_empty())
            {
                return Err(format!("evaluation missing {key}"));
            }
        }
    }
    if !["keep", "revise", "retire", "create"].contains(&action) {
        return Err("unknown evaluation action".into());
    }
    if action == "revise" {
        let replacement = if kind == "agent" {
            serde_json::to_value(parse_assigned_agent(
                &verdict["replacement"],
                &state.config,
                Some(before),
            )?)
            .map_err(|error| error.to_string())?
        } else {
            let mut workflow: Workflow = serde_json::from_value(verdict["replacement"].clone())
                .map_err(|error| error.to_string())?;
            workflow.id = id.to_string();
            validate_workflow_agents(state, &workflow)?;
            serde_json::to_value(workflow).map_err(|error| error.to_string())?
        };
        verdict["replacement"] = replacement;
    } else if action == "retire" {
        verdict["replacement"] = Value::Null;
    }
    if state.portfolio.generation() != generation {
        return Err("reset during evaluation".into());
    }
    trace.finish("completed", &json!({"suggestion":verdict}), None)?;
    Ok(verdict)
}

fn bound_definition_strings(value: &mut Value, max_chars: usize) -> bool {
    match value {
        Value::String(text) if text.chars().count() > max_chars => {
            let prefix = text.chars().take(max_chars).collect::<String>();
            *text = format!("{prefix}…[definition excerpt truncated]");
            true
        }
        Value::Array(items) => {
            let mut truncated = false;
            for item in items {
                truncated |= bound_definition_strings(item, max_chars);
            }
            truncated
        }
        Value::Object(fields) => {
            let mut truncated = false;
            for item in fields.values_mut() {
                truncated |= bound_definition_strings(item, max_chars);
            }
            truncated
        }
        _ => false,
    }
}

fn bounded_suggestion_input(value: &mut Value) -> String {
    while serde_json::to_vec(value).map_or(0, |bytes| bytes.len()) > MAX_SUGGESTION_INPUT_BYTES {
        sync_suggestion_coverage(value);
        value["coverage"]["complete"] = Value::Bool(false);
        if !value["definition"].is_null() {
            value["definition"] = Value::Null;
            value["coverage"]["definition_omitted_from_context"] = Value::Bool(true);
            continue;
        }
        if remove_first_nested_array_item(
            value,
            &[
                "conversation_execution_evidence",
                "conversation_execution",
                "events",
            ],
        ) || remove_first_nested_array_item(
            value,
            &[
                "conversation_execution_evidence",
                "conversation_execution",
                "spans",
            ],
        ) || remove_first_nested_array_item(
            value,
            &[
                "conversation_execution_evidence",
                "conversation_execution",
                "runs",
            ],
        ) || remove_nested_prompt(value)
        {
            sync_suggestion_coverage(value);
            continue;
        }

        let mut shortened = false;
        for key in [
            "conversation_execution_evidence",
            "whole_conversation_user_prompts",
        ] {
            if let Some(text) = value[key].as_str().filter(|text| text.len() > 128) {
                let target = text.len() / 2;
                let mut boundary = target.min(text.len());
                while !text.is_char_boundary(boundary) {
                    boundary -= 1;
                }
                value[key] = json!(format!(
                    "{}…[bounded model-context excerpt]",
                    &text[..boundary]
                ));
                value["coverage"]["evidence_omitted_from_context"] = Value::Bool(true);
                shortened = true;
                break;
            }
        }
        if shortened {
            continue;
        }
        if !value["conversation_execution_evidence"].is_null()
            || !value["whole_conversation_user_prompts"].is_null()
        {
            value["conversation_execution_evidence"] = Value::Null;
            value["whole_conversation_user_prompts"] = Value::Null;
            value["coverage"]["evidence_omitted_from_context"] = Value::Bool(true);
            continue;
        }
        break;
    }
    sync_suggestion_coverage(value);
    serde_json::to_string(value).unwrap_or_else(|_| "{}".into())
}

fn remove_first_nested_array_item(value: &mut Value, path: &[&str]) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };
    let mut target = value;
    for key in parents {
        let Some(next) = target.get_mut(*key) else {
            return false;
        };
        target = next;
    }
    let Some(items) = target.get_mut(*last).and_then(Value::as_array_mut) else {
        return false;
    };
    if items.is_empty() {
        return false;
    }
    items.remove(0);
    true
}

fn remove_nested_prompt(value: &mut Value) -> bool {
    let Some(prompts) = value
        .get_mut("whole_conversation_user_prompts")
        .and_then(|input| input.get_mut("user_prompts"))
        .and_then(Value::as_array_mut)
    else {
        return false;
    };
    if prompts.len() > 1 {
        prompts.remove(1);
    } else if !prompts.is_empty() {
        prompts.remove(0);
    } else {
        return false;
    }
    true
}

fn sync_suggestion_coverage(value: &mut Value) {
    let mut nested_complete = true;
    for key in [
        "whole_conversation_user_prompts",
        "conversation_execution_evidence",
    ] {
        let Some(nested) = value.get_mut(key).filter(|nested| nested.is_object()) else {
            nested_complete = false;
            continue;
        };
        crate::improvement::sync_evidence_coverage(nested);
        nested_complete &= nested
            .get("coverage")
            .and_then(|coverage| coverage.get("complete"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
    }
    if !nested_complete {
        value["coverage"]["complete"] = Value::Bool(false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn all_seven_provisions_are_required() {
        let mut value = json!({"name":"a","purpose":"p","provisions":{}});
        for key in PROVISIONS {
            value["provisions"][key] = json!(format!("substantive {key}"));
        }
        assert!(
            parse_agent(&value, "m")
                .unwrap()
                .system_prompt
                .contains(RHYTHM)
        );
        for key in PROVISIONS {
            let mut invalid = value.clone();
            invalid["provisions"].as_object_mut().unwrap().remove(key);
            assert!(parse_agent(&invalid, "m").is_err());
        }
    }

    #[test]
    fn create_suggestion_verdict_is_requirements_only_and_bounded() {
        let valid = json!({
            "action":"create",
            "kind":"workflow",
            "requirements":"Reusable workflow requirements"
        });
        assert!(validate_create_suggestion_verdict(&valid).is_ok());
        assert!(
            validate_create_suggestion_verdict(&json!({
                "action":"create",
                "kind":"agent",
                "requirements":"Reusable agent",
                "reason":"extra fields are not part of the contract"
            }))
            .is_err()
        );
        assert!(
            validate_create_suggestion_verdict(&json!({
                "action":"create",
                "kind":"system",
                "requirements":"Reusable definition"
            }))
            .is_err()
        );
        assert!(
            validate_create_suggestion_verdict(&json!({
                "action":"create",
                "kind":"agent",
                "requirements":"   "
            }))
            .is_err()
        );
        assert!(
            validate_create_suggestion_verdict(&json!({
                "action":"create",
                "kind":"agent",
                "requirements":"x".repeat(MAX_SUGGESTION_INPUT_BYTES + 1)
            }))
            .is_err()
        );
    }

    #[test]
    fn create_is_only_advertised_by_the_read_only_suggestion_evaluator() {
        assert!(suggestion_evaluator_prompt().contains("For create, return exactly"));
        assert!(!evaluator_prompt().contains("For create"));
    }

    #[test]
    fn bounded_nested_evidence_coverage_matches_final_prompt_span_event_and_run_arrays() {
        let prompts = (0..80)
            .map(|turn| json!({"turn":turn + 1,"text":"prompt text ".repeat(100)}))
            .collect::<Vec<_>>();
        let runs = (0..80)
            .map(|run| json!(format!("run-{run}")))
            .collect::<Vec<_>>();
        let spans = (0..80)
            .map(|_| json!({"kind":"agent_call","output":"span evidence ".repeat(100)}))
            .collect::<Vec<_>>();
        let events = (0..80)
            .map(|_| json!({"kind":"trace_event","data":"event evidence ".repeat(100)}))
            .collect::<Vec<_>>();
        let mut input = json!({
            "definition":{"large":"evaluated definition"},
            "whole_conversation_user_prompts":{
                "user_prompts":prompts,
                "coverage":{"complete":true,"available":80,"included":80}
            },
            "conversation_execution_evidence":{
                "conversation_execution":{"runs":runs,"spans":spans,"events":events},
                "coverage":{
                    "complete":true,
                    "runs_available":80,"runs_included":80,
                    "spans_available_in_sampled_runs":80,"spans_included":80,
                    "events_available_in_sampled_runs":80,"events_included":80
                }
            },
            "coverage":{"complete":true,"definition_complete":true}
        });

        let bounded = bounded_suggestion_input(&mut input);
        let parsed: Value = serde_json::from_str(&bounded).unwrap();
        let prompt_input = &parsed["whole_conversation_user_prompts"];
        let final_prompts = prompt_input["user_prompts"].as_array().unwrap();
        assert_eq!(
            prompt_input["coverage"]["included"].as_u64().unwrap(),
            final_prompts.len() as u64
        );
        assert_eq!(
            prompt_input["coverage"]["omitted_from_context"]
                .as_u64()
                .unwrap(),
            80 - final_prompts.len() as u64
        );
        let execution_input = &parsed["conversation_execution_evidence"];
        let execution = &execution_input["conversation_execution"];
        for (array, included, available, omitted) in [
            (
                "runs",
                "runs_included",
                "runs_available",
                "runs_omitted_from_context",
            ),
            (
                "spans",
                "spans_included",
                "spans_available_in_sampled_runs",
                "spans_omitted_from_context",
            ),
            (
                "events",
                "events_included",
                "events_available_in_sampled_runs",
                "events_omitted_from_context",
            ),
        ] {
            let count = execution[array].as_array().unwrap().len() as u64;
            assert_eq!(execution_input["coverage"][included].as_u64(), Some(count));
            assert_eq!(execution_input["coverage"][available].as_u64(), Some(80));
            assert_eq!(
                execution_input["coverage"][omitted].as_u64(),
                Some(80 - count)
            );
        }
        assert_eq!(parsed["coverage"]["complete"], false);
        assert!(bounded.len() <= MAX_SUGGESTION_INPUT_BYTES);
    }
}
