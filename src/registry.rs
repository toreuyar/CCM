//! Model-owned semantic reuse/consolidation with revision-checked atomic commits.
use crate::{
    meta,
    observe::{now_ms, snapshot_conn},
    portfolio::Portfolio,
    state::AppState,
    upstream,
    workflow::Workflow,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::sync::Arc;

const MAX_CONSOLIDATION_CANDIDATES_PER_KIND: i64 = 8;
const MAX_CONSOLIDATION_DEFINITION_CHARS: usize = 4096;
const MAX_CONSOLIDATION_INPUT_BYTES: usize = 64 * 1024;

fn snapshot(conn: &Connection, workforce: Option<&str>) -> Result<Value, String> {
    let agents=conn.prepare("SELECT id,name,purpose,system_prompt,model,provider FROM agents WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1) ORDER BY created_at,id").map_err(|e|e.to_string())?
        .query_map([workforce],|r|Ok(json!({"id":r.get::<_,String>(0)?,"name":r.get::<_,String>(1)?,"purpose":r.get::<_,String>(2)?,"system_prompt":r.get::<_,String>(3)?,"model":r.get::<_,String>(4)?,"provider":r.get::<_,String>(5)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
    let workflows = conn
        .prepare("SELECT definition FROM workflows WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1) ORDER BY id")
        .map_err(|e| e.to_string())?
        .query_map([workforce], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|s| serde_json::from_str::<Value>(&s).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({"agents":agents,"workflows":workflows}))
}
fn definitions<'a>(registry: &'a Value, kind: &str) -> Result<&'a Vec<Value>, String> {
    let key = match kind {
        "agent" => "agents",
        "workflow" => "workflows",
        _ => return Err("invalid definition kind".into()),
    };
    registry[key]
        .as_array()
        .ok_or_else(|| "invalid registry snapshot".into())
}
fn find<'a>(registry: &'a Value, kind: &str, id: &str) -> Result<&'a Value, String> {
    definitions(registry, kind)?
        .iter()
        .find(|v| v["id"] == id)
        .ok_or_else(|| format!("unknown active {kind}: {id}"))
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value[key]
        .as_str()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| format!("missing {key}"))
}
fn revision(conn: &Connection) -> Result<i64, String> {
    conn.query_row("SELECT value FROM registry_revision WHERE id=1", [], |r| {
        r.get(0)
    })
    .map_err(|e| e.to_string())
}

fn consolidation_snapshot(
    portfolio: &Portfolio,
) -> Result<(i64, Value, std::collections::HashMap<String, bool>), String> {
    let conn = portfolio.conn.lock().unwrap();
    let version = revision(&conn)?;
    let agents_available: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM agents WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1)",
            [portfolio.workforce_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let workflows_available: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM workflows WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1)",
            [portfolio.workforce_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let mut complete = std::collections::HashMap::new();
    let mut agent_statement = conn
        .prepare(
            "SELECT id,name,purpose,substr(system_prompt,1,?2+1),model,provider,
               length(system_prompt)>?2
             FROM agents WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1)
             ORDER BY created_at DESC,id DESC LIMIT ?3",
        )
        .map_err(|error| error.to_string())?;
    let agents = agent_statement
        .query_map(
            params![
                portfolio.workforce_scope(),
                MAX_CONSOLIDATION_DEFINITION_CHARS as i64,
                MAX_CONSOLIDATION_CANDIDATES_PER_KIND
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, bool>(6)?,
                ))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut agent_values = Vec::with_capacity(agents.len());
    let mut agent_truncated = false;
    for (id, name, purpose, system_prompt, model, provider, truncated) in agents {
        agent_truncated |= truncated;
        complete.insert(format!("agent:{id}"), !truncated);
        agent_values.push(json!({
            "id":id,"name":name,"purpose":purpose,"system_prompt":system_prompt,
            "model":model,"provider":provider,"review_complete":!truncated
        }));
    }
    drop(agent_statement);

    let mut workflow_statement = conn
        .prepare(
            "SELECT id,substr(definition,1,?1+1),length(definition)>?1
             FROM workflows WHERE retired=0 AND (?2 IS NULL OR workforce_id=?2)
             ORDER BY id DESC LIMIT ?3",
        )
        .map_err(|error| error.to_string())?;
    let workflows = workflow_statement
        .query_map(
            params![
                MAX_CONSOLIDATION_DEFINITION_CHARS as i64,
                portfolio.workforce_scope(),
                MAX_CONSOLIDATION_CANDIDATES_PER_KIND
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, bool>(2)?,
                ))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut workflow_values = Vec::with_capacity(workflows.len());
    let mut workflow_truncated = false;
    for (id, definition, truncated) in workflows {
        workflow_truncated |= truncated;
        complete.insert(format!("workflow:{id}"), !truncated);
        if truncated {
            workflow_values
                .push(json!({"id":id,"definition_prefix":definition,"review_complete":false}));
        } else {
            let mut value: Value =
                serde_json::from_str(&definition).map_err(|error| error.to_string())?;
            value["review_complete"] = Value::Bool(true);
            workflow_values.push(value);
        }
    }
    drop(workflow_statement);
    let agents_included = agent_values.len() as i64;
    let workflows_included = workflow_values.len() as i64;
    let roster_complete = agents_available <= MAX_CONSOLIDATION_CANDIDATES_PER_KIND
        && workflows_available <= MAX_CONSOLIDATION_CANDIDATES_PER_KIND
        && !agent_truncated
        && !workflow_truncated;
    Ok((
        version,
        json!({
            "agents":agent_values,
            "workflows":workflow_values,
            "coverage":{
                "complete":roster_complete,
                "agents_available":agents_available,
                "agents_included":agents_included,
                "workflows_available":workflows_available,
                "workflows_included":workflows_included,
                "definition_text_limited":true,
                "sampling":"most recently created active agents and workflows"
            }
        }),
        complete,
    ))
}

fn bounded_consolidation_input(mut value: Value) -> String {
    loop {
        sync_consolidation_coverage(&mut value);
        if serde_json::to_vec(&value).map_or(0, |bytes| bytes.len())
            <= MAX_CONSOLIDATION_INPUT_BYTES
        {
            break;
        }
        value["coverage"]["complete"] = Value::Bool(false);
        value["registry"]["coverage"]["complete"] = Value::Bool(false);
        if remove_nested_array_item(
            &mut value,
            &["checkpoint_evidence", "conversation_execution", "events"],
        ) || remove_nested_array_item(
            &mut value,
            &["checkpoint_evidence", "conversation_execution", "spans"],
        ) || remove_nested_array_item(
            &mut value,
            &["checkpoint_evidence", "conversation_execution", "runs"],
        ) || remove_checkpoint_prompt(&mut value)
        {
            continue;
        }
        if let Some(workflows) = value
            .get_mut("registry")
            .and_then(|v| v.get_mut("workflows"))
            .and_then(Value::as_array_mut)
            && !workflows.is_empty()
        {
            workflows.pop();
            continue;
        }
        if let Some(agents) = value
            .get_mut("registry")
            .and_then(|v| v.get_mut("agents"))
            .and_then(Value::as_array_mut)
            && !agents.is_empty()
        {
            agents.pop();
            continue;
        }
        break;
    }
    sync_consolidation_coverage(&mut value);
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
    if text.len() <= MAX_CONSOLIDATION_INPUT_BYTES {
        text
    } else {
        value["registry"]["agents"] = json!([]);
        value["registry"]["workflows"] = json!([]);
        if let Some(evidence) = value.get_mut("checkpoint_evidence") {
            if evidence.is_object() {
                evidence["user_prompts"] = json!([]);
                evidence["conversation_execution"]["runs"] = json!([]);
                evidence["conversation_execution"]["spans"] = json!([]);
                evidence["conversation_execution"]["events"] = json!([]);
            } else {
                *evidence = Value::Null;
            }
        }
        value["coverage"]["complete"] = Value::Bool(false);
        value["coverage"]["evidence_omitted_from_context"] = Value::Bool(true);
        value["registry"]["coverage"]["complete"] = Value::Bool(false);
        sync_consolidation_coverage(&mut value);
        serde_json::to_string(&value).unwrap_or_else(|_| "{}".into())
    }
}

fn remove_nested_array_item(value: &mut Value, path: &[&str]) -> bool {
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

fn remove_checkpoint_prompt(value: &mut Value) -> bool {
    let Some(prompts) = value
        .get_mut("checkpoint_evidence")
        .and_then(|evidence| evidence.get_mut("user_prompts"))
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

fn sync_consolidation_coverage(value: &mut Value) {
    let agents_included = value
        .pointer("/registry/agents")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    let workflows_included = value
        .pointer("/registry/workflows")
        .and_then(Value::as_array)
        .map_or(0, Vec::len);
    for coverage_path in ["/coverage", "/registry/coverage"] {
        if let Some(coverage) = value
            .pointer_mut(coverage_path)
            .and_then(Value::as_object_mut)
        {
            let agents_available = coverage
                .get("agents_available")
                .and_then(Value::as_u64)
                .unwrap_or(agents_included as u64);
            let workflows_available = coverage
                .get("workflows_available")
                .and_then(Value::as_u64)
                .unwrap_or(workflows_included as u64);
            let agents_omitted = agents_available.saturating_sub(agents_included as u64);
            let workflows_omitted = workflows_available.saturating_sub(workflows_included as u64);
            coverage.insert("agents_included".into(), json!(agents_included));
            coverage.insert("workflows_included".into(), json!(workflows_included));
            coverage.insert("agents_omitted_from_context".into(), json!(agents_omitted));
            coverage.insert(
                "workflows_omitted_from_context".into(),
                json!(workflows_omitted),
            );
            if agents_omitted > 0 || workflows_omitted > 0 {
                coverage.insert("complete".into(), Value::Bool(false));
            }
        }
    }
    if let Some(evidence) = value
        .get_mut("checkpoint_evidence")
        .filter(|evidence| evidence.is_object())
    {
        crate::improvement::sync_evidence_coverage(evidence);
        let evidence_complete = evidence
            .pointer("/coverage/complete")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if !evidence_complete {
            value["coverage"]["complete"] = Value::Bool(false);
        }
    }
}

// Mutation and lineage are one transaction, including a durable completion receipt.
#[allow(clippy::too_many_arguments)]
fn audit(
    conn: &Connection,
    request: &str,
    kind: &str,
    id: &str,
    before: Option<&Value>,
    after: Option<&Value>,
    action: &str,
    evidence: &Value,
    workforce: Option<&str>,
    owner: Option<&str>,
) -> Result<(), String> {
    let before = before
        .map(|v| snapshot_conn(conn, id, kind, v, workforce))
        .transpose()
        .map_err(|e| e.to_string())?;
    let after = after
        .map(|v| snapshot_conn(conn, id, kind, v, workforce))
        .transpose()
        .map_err(|e| e.to_string())?;
    let context: Option<(String, String)> = conn
        .query_row(
            "SELECT q.run_id,q.current_span FROM trace_requests q JOIN trace_runs r ON r.id=q.run_id
             WHERE q.id=?1 AND (?2 IS NULL OR r.workforce_id=?2) AND (?3 IS NULL OR r.owner_key_id=?3)",
            params![request,workforce,owner],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let (run, span) = context.unwrap_or_default();
    conn.execute("INSERT INTO definition_changes(entity_id,kind,before_version,after_version,run_id,span_id,action,evidence,applied,at_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?10,?11)",params![id,kind,before,after,run,span,action,evidence.to_string(),now_ms(),workforce,owner]).map_err(|e|e.to_string())?;
    conn.execute("INSERT INTO trace_events(run_id,span_id,kind,at_ms,data,workforce_id,owner_key_id) VALUES(?1,?2,'definition.changed',?3,?4,?5,?6)",params![run,span,now_ms(),json!({"entity_id":id,"kind":kind,"action":action,"evidence":evidence,"applied":true}).to_string(),workforce,owner]).map_err(|e|e.to_string())?;
    Ok(())
}

impl Portfolio {
    pub fn registry_snapshot(&self) -> Result<(i64, Value), String> {
        let conn = self.conn.lock().unwrap();
        Ok((revision(&conn)?, snapshot(&conn, self.workforce_scope())?))
    }
    pub fn creation_receipt(&self, job: &str, index: usize) -> Result<Option<Value>, String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT kind,entity_id FROM creation_receipts WHERE job_id=?1 AND order_index=?2
             AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4)",
            params![job, index, self.workforce_scope(), self.key_scope()],
            |r| Ok(json!({"kind":r.get::<_,String>(0)?,"id":r.get::<_,String>(1)?,"receipt":true})),
        )
        .optional()
        .map_err(|e| e.to_string())
    }
}

#[derive(Clone, Debug)]
struct CreationSuggestion {
    id: String,
    job_id: String,
    workforce_id: String,
    owner_key_id: String,
    kind: String,
    entity_id: String,
    proposal: String,
    before_definition: String,
    status: String,
    declassified_ms: Option<i64>,
    idempotency_key: String,
    plan_index: usize,
    writer_key_id: String,
}

/// Keep explicit create-apply responses to the peer-visible resource result.
/// `handle` is the source ID for its owner or the opaque share ID for a peer.
/// Only the source owner may receive the private suggestion/job status fields.
fn creation_suggestion_apply_result(
    handle: &str,
    suggestion: &CreationSuggestion,
    entity_id: &str,
    job_status: Option<&str>,
) -> Value {
    let mut result = json!({
        "id":handle,
        "status":"applied",
        "action":"create",
        "kind":suggestion.kind,
        "entity_id":entity_id,
        "applied":true,
        "receipt":true
    });
    if suggestion.owner_key_id == suggestion.writer_key_id {
        result["suggestion_id"] = json!(suggestion.id);
        if let Some(job_status) = job_status {
            result["job_status"] = json!(job_status);
        }
    }
    result
}

#[derive(Clone, Debug)]
enum CreationRegistration {
    Ordinary {
        receipt: Option<(String, usize)>,
    },
    AutoPrivate {
        receipt: (String, usize),
        authority: crate::improvement::ApplicationAuthority,
    },
    ReleasedSuggestion(CreationSuggestion),
}

#[derive(Debug)]
struct PlannedCreation {
    kind: String,
    requirements: String,
}

fn creation_idempotency_key(job_id: &str, kind: &str, index: usize) -> String {
    format!("{job_id}:create:{kind}:{index}")
}

fn creation_plan_index(job_id: &str, kind: &str, key: &str) -> Result<usize, String> {
    let prefix = format!("{job_id}:create:{kind}:");
    let index = key
        .strip_prefix(&prefix)
        .and_then(|index| index.parse().ok())
        .ok_or("creation suggestion has an invalid plan idempotency key")?;
    if creation_idempotency_key(job_id, kind, index) != key {
        return Err("creation suggestion has a non-canonical plan idempotency key".into());
    }
    Ok(index)
}

fn read_creation_plan(
    conn: &Connection,
    job_id: &str,
    workforce_id: &str,
    owner_key_id: &str,
) -> Result<Vec<PlannedCreation>, String> {
    let raw: String = conn
        .query_row(
            "SELECT plan FROM workforce_plans WHERE job_id=?1 AND workforce_id=?2 AND owner_key_id=?3",
            params![job_id, workforce_id, owner_key_id],
            |row| row.get(0),
        )
        .map_err(|error| format!("missing scoped creation plan: {error}"))?;
    let value: Value = serde_json::from_str(&raw).map_err(|error| error.to_string())?;
    let entries = value
        .as_array()
        .ok_or("persisted creation plan is not an array")?;
    let mut plan = Vec::with_capacity(entries.len());
    for item in entries {
        let fields = item
            .as_object()
            .ok_or("persisted creation plan item is not an object")?;
        if fields.len() != 3
            || !fields.contains_key("action")
            || !fields.contains_key("kind")
            || !fields.contains_key("requirements")
            || item["action"] != "create"
        {
            return Err(
                "create plan items must contain exactly action, kind, and requirements".into(),
            );
        }
        let kind = item["kind"]
            .as_str()
            .filter(|kind| ["agent", "workflow"].contains(kind))
            .ok_or("creation plan item has an invalid kind")?;
        let requirements = item["requirements"]
            .as_str()
            .filter(|requirements| !requirements.trim().is_empty())
            .ok_or("creation plan item has empty requirements")?;
        plan.push(PlannedCreation {
            kind: kind.to_owned(),
            requirements: requirements.to_owned(),
        });
    }
    if plan.is_empty() {
        return Err("persisted creation plan is empty".into());
    }
    Ok(plan)
}

fn validate_creation_plan_item(
    conn: &Connection,
    job_id: &str,
    workforce_id: &str,
    owner_key_id: &str,
    index: usize,
    kind: &str,
    requirements: &str,
) -> Result<Vec<PlannedCreation>, String> {
    let plan = read_creation_plan(conn, job_id, workforce_id, owner_key_id)?;
    let item = plan
        .get(index)
        .ok_or("creation plan receipt index is outside the persisted plan")?;
    if item.kind != kind || item.requirements != requirements {
        return Err("creation request does not match its persisted plan item".into());
    }
    Ok(plan)
}

fn source_creation_receipt(
    conn: &Connection,
    job_id: &str,
    index: usize,
    workforce_id: &str,
    owner_key_id: &str,
) -> Result<Option<Value>, String> {
    conn.query_row(
        "SELECT kind,entity_id FROM creation_receipts
         WHERE job_id=?1 AND order_index=?2 AND workforce_id=?3 AND owner_key_id=?4",
        params![job_id, index, workforce_id, owner_key_id],
        |row| {
            Ok(json!({
                "kind":row.get::<_,String>(0)?,
                "id":row.get::<_,String>(1)?,
                "receipt":true
            }))
        },
    )
    .optional()
    .map_err(|error| error.to_string())
}

fn validate_creation_suggestion(value: &Value, kind: &str) -> Result<String, String> {
    let fields = value
        .as_object()
        .ok_or("create suggestion proposal must be a JSON object")?;
    if fields.len() != 3
        || !fields.contains_key("action")
        || !fields.contains_key("kind")
        || !fields.contains_key("requirements")
        || value["action"] != "create"
        || value["kind"] != kind
    {
        return Err("create suggestion proposal does not match its kind/action contract".into());
    }
    value["requirements"]
        .as_str()
        .filter(|requirements| !requirements.trim().is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "create suggestion has empty requirements".into())
}

fn load_creation_suggestion(
    state: &Arc<AppState>,
    handle: &str,
) -> Result<CreationSuggestion, String> {
    let auth = state
        .auth
        .as_ref()
        .ok_or("create suggestion application requires an authenticated writer")?;
    auth.require(crate::auth::RequiredPermission::RosterWrite)
        .map_err(str::to_owned)?;
    if state.portfolio.workforce_scope() != Some(auth.workforce_id.as_str())
        || state.portfolio.key_scope() != Some(auth.key_id.as_str())
    {
        return Err("create suggestion scope does not match the current writer".into());
    }
    let conn = state.portfolio.conn.lock().unwrap();
    let select = "SELECT id,job_id,workforce_id,owner_key_id,entity_kind,entity_id,
                         proposal,before_definition,status,declassified_ms,idempotency_key
                  FROM improvement_suggestions";
    let own_sql =
        format!("{select} WHERE id=?1 AND action='create' AND workforce_id=?2 AND owner_key_id=?3");
    let own = conn
        .query_row(
            &own_sql,
            params![handle, auth.workforce_id, auth.key_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, String>(10)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let row = if let Some(own) = own {
        own
    } else {
        let shared_sql = format!(
            "{select} WHERE share_id=?1 AND action='create' AND workforce_id=?2 AND owner_key_id<>?3"
        );
        conn.query_row(
            &shared_sql,
            params![handle, auth.workforce_id, auth.key_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, String>(8)?,
                    row.get::<_, Option<i64>>(9)?,
                    row.get::<_, String>(10)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?
        .ok_or("unknown create suggestion")?
    };
    let (
        id,
        job_id,
        workforce_id,
        owner_key_id,
        kind,
        entity_id,
        proposal,
        before_definition,
        status,
        declassified_ms,
        idempotency_key,
    ) = row;
    let cross_key = owner_key_id != auth.key_id;
    require_current_roster_writer(&conn, &workforce_id, &auth.key_id)?;
    if cross_key && declassified_ms.is_none() {
        return Err("source key release is required before cross-key creation apply".into());
    }
    if cross_key {
        require_current_roster_reader(&conn, &workforce_id, &auth.key_id)?;
    }
    if before_definition != "null" {
        return Err("create suggestion has an invalid before-definition sentinel".into());
    }
    let proposal_value: Value =
        serde_json::from_str(&proposal).map_err(|error| error.to_string())?;
    let requirements = validate_creation_suggestion(&proposal_value, &kind)?;
    let plan_index = creation_plan_index(&job_id, &kind, &idempotency_key)?;
    validate_creation_plan_item(
        &conn,
        &job_id,
        &workforce_id,
        &owner_key_id,
        plan_index,
        &kind,
        &requirements,
    )?;
    if status == "pending" {
        let active: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM improvement_jobs WHERE id=?1 AND workforce_id=?2
                 AND owner_key_id=?3 AND status IN ('running','suggested')
                 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
                params![job_id, workforce_id, owner_key_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if !active {
            return Err("create suggestion job is no longer active".into());
        }
        if entity_id != "" {
            return Err("pending create suggestion already has an entity receipt".into());
        }
    } else if status == "applied" {
        if entity_id.is_empty() {
            return Err("applied create suggestion is missing its entity receipt".into());
        }
    } else {
        return Err(format!("create suggestion is {status}, not pending"));
    }
    Ok(CreationSuggestion {
        id,
        job_id,
        workforce_id,
        owner_key_id,
        kind,
        entity_id,
        proposal,
        before_definition,
        status,
        declassified_ms,
        idempotency_key,
        plan_index,
        writer_key_id: auth.key_id.clone(),
    })
}

fn require_current_roster_writer(
    conn: &Connection,
    workforce_id: &str,
    writer_key_id: &str,
) -> Result<(), String> {
    let allowed: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM api_keys WHERE id=?1 AND workforce_id=?2
             AND revoked_ms IS NULL AND roster_write=1)",
            params![writer_key_id, workforce_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if allowed {
        Ok(())
    } else {
        Err("current workforce writer permission is required".into())
    }
}

fn require_current_roster_reader(
    conn: &Connection,
    workforce_id: &str,
    writer_key_id: &str,
) -> Result<(), String> {
    let allowed: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM api_keys WHERE id=?1 AND workforce_id=?2
             AND revoked_ms IS NULL AND roster_read=1)",
            params![writer_key_id, workforce_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if allowed {
        Ok(())
    } else {
        Err("current workforce roster-read permission is required".into())
    }
}

fn validate_auto_private_creation(
    state: &AppState,
    tx: &rusqlite::Transaction<'_>,
    authority: &crate::improvement::ApplicationAuthority,
    receipt: &(String, usize),
    kind: &str,
    requirements: &str,
) -> Result<Vec<PlannedCreation>, String> {
    let crate::improvement::ApplicationAuthority::AutoPrivate {
        source_key_id,
        workforce_id,
        job_id,
    } = authority
    else {
        return Err("registry auto-creation requires AutoPrivate authority".into());
    };
    if receipt.0 != *job_id
        || state.portfolio.workforce_scope() != Some(workforce_id.as_str())
        || state.portfolio.key_scope() != Some(source_key_id.as_str())
        || !state.auth.as_ref().is_some_and(|auth| {
            auth.key_id == *source_key_id
                && auth.workforce_id == *workforce_id
                && auth.permissions.roster_write
                && auth.permissions.improvement_contribute
        })
    {
        return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    }
    crate::improvement::require_auto_private_mutation(tx, workforce_id, source_key_id)
        .map_err(|_| crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.to_owned())?;
    let active: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_jobs WHERE id=?1 AND workforce_id=?2
             AND owner_key_id=?3 AND auto_apply_private=1 AND status IN ('running','suggested')
             AND epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
            params![job_id, workforce_id, source_key_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if !active {
        return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    }
    validate_creation_plan_item(
        tx,
        job_id,
        workforce_id,
        source_key_id,
        receipt.1,
        kind,
        requirements,
    )
}

fn validate_suggestion_creation_tx(
    state: &AppState,
    tx: &rusqlite::Transaction<'_>,
    suggestion: &CreationSuggestion,
    kind: &str,
    requirements: &str,
) -> Result<Vec<PlannedCreation>, String> {
    if state.portfolio.workforce_scope() != Some(suggestion.workforce_id.as_str())
        || state.portfolio.key_scope() != Some(suggestion.writer_key_id.as_str())
        || suggestion.kind != kind
        || suggestion.entity_id != ""
        || suggestion.before_definition != "null"
        || suggestion.status != "pending"
        || (suggestion.owner_key_id != suggestion.writer_key_id
            && suggestion.declassified_ms.is_none())
    {
        return Err("create suggestion is no longer applicable".into());
    }
    require_current_roster_writer(tx, &suggestion.workforce_id, &suggestion.writer_key_id)?;
    if suggestion.owner_key_id != suggestion.writer_key_id {
        require_current_roster_reader(tx, &suggestion.workforce_id, &suggestion.writer_key_id)?;
    }
    let persisted: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_suggestions s
             JOIN improvement_jobs j ON j.id=s.job_id
             WHERE s.id=?1 AND s.job_id=?2 AND s.workforce_id=?3 AND s.owner_key_id=?4
               AND s.entity_kind=?5 AND s.entity_id='' AND s.action='create'
               AND s.proposal=?6 AND s.before_definition='null' AND s.status='pending'
               AND s.declassified_ms IS ?7 AND s.idempotency_key=?8
               AND j.workforce_id=?3 AND j.owner_key_id=?4 AND j.status IN ('running','suggested')
               AND j.epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
            params![
                suggestion.id,
                suggestion.job_id,
                suggestion.workforce_id,
                suggestion.owner_key_id,
                suggestion.kind,
                suggestion.proposal,
                suggestion.declassified_ms,
                suggestion.idempotency_key
            ],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if !persisted {
        return Err("create suggestion changed during generator work".into());
    }
    validate_creation_plan_item(
        tx,
        &suggestion.job_id,
        &suggestion.workforce_id,
        &suggestion.owner_key_id,
        suggestion.plan_index,
        kind,
        requirements,
    )
}

fn update_creation_suggestion(
    tx: &rusqlite::Transaction<'_>,
    suggestion: &CreationSuggestion,
    entity_id: &str,
    request: &str,
) -> Result<(), String> {
    let changed = tx
        .execute(
            "UPDATE improvement_suggestions SET entity_id=?2,status='applied',applied_ms=?3,
             application_request=?4
             WHERE id=?1 AND job_id=?5 AND workforce_id=?6 AND owner_key_id=?7
               AND entity_kind=?8 AND entity_id='' AND action='create' AND proposal=?9
               AND before_definition='null' AND status='pending' AND idempotency_key=?10",
            params![
                suggestion.id,
                entity_id,
                now_ms(),
                request,
                suggestion.job_id,
                suggestion.workforce_id,
                suggestion.owner_key_id,
                suggestion.kind,
                suggestion.proposal,
                suggestion.idempotency_key
            ],
        )
        .map_err(|error| error.to_string())?;
    if changed == 1 {
        Ok(())
    } else {
        Err("create suggestion was already applied or changed".into())
    }
}

fn auto_private_creation_suggestion(
    tx: &rusqlite::Transaction<'_>,
    job_id: &str,
    workforce_id: &str,
    owner_key_id: &str,
    index: usize,
    kind: &str,
    requirements: &str,
) -> Result<Option<CreationSuggestion>, String> {
    let key = creation_idempotency_key(job_id, kind, index);
    let row: Option<(String, String, String, String, Option<i64>)> = tx
        .query_row(
            "SELECT id,proposal,before_definition,entity_id,declassified_ms
             FROM improvement_suggestions WHERE job_id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND entity_kind=?4 AND action='create' AND idempotency_key=?5",
            params![job_id, workforce_id, owner_key_id, kind, key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some((id, proposal, before_definition, entity_id, declassified_ms)) = row else {
        return Ok(None);
    };
    let proposal_value: Value =
        serde_json::from_str(&proposal).map_err(|error| error.to_string())?;
    if validate_creation_suggestion(&proposal_value, kind)? != requirements
        || before_definition != "null"
        || !entity_id.is_empty()
    {
        return Err("automatic create suggestion does not match its durable plan receipt".into());
    }
    Ok(Some(CreationSuggestion {
        id,
        job_id: job_id.to_owned(),
        workforce_id: workforce_id.to_owned(),
        owner_key_id: owner_key_id.to_owned(),
        kind: kind.to_owned(),
        entity_id,
        proposal,
        before_definition,
        status: "pending".into(),
        declassified_ms,
        idempotency_key: key,
        plan_index: index,
        writer_key_id: owner_key_id.to_owned(),
    }))
}

fn finish_creation_job(
    tx: &rusqlite::Transaction<'_>,
    job_id: &str,
    workforce_id: &str,
    owner_key_id: &str,
    auto_private: bool,
) -> Result<String, String> {
    let plan = read_creation_plan(tx, job_id, workforce_id, owner_key_id)?;
    let mut fulfilled = Vec::new();
    let mut pending_indices = Vec::new();
    for (index, item) in plan.iter().enumerate() {
        match source_creation_receipt(tx, job_id, index, workforce_id, owner_key_id)? {
            Some(receipt) if receipt["kind"] == item.kind => fulfilled.push(json!({
                "order":index,
                "kind":item.kind,
                "id":receipt["id"]
            })),
            Some(_) => return Err("creation receipt kind does not match its plan item".into()),
            None => pending_indices.push(index),
        }
    }
    let pending_suggestions: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM improvement_suggestions WHERE job_id=?1 AND workforce_id=?2
             AND owner_key_id=?3 AND status='pending'",
            params![job_id, workforce_id, owner_key_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let complete = pending_indices.is_empty() && pending_suggestions == 0;
    let status = if complete {
        "completed"
    } else if auto_private {
        // An unfinished private plan must remain restart-recoverable. Startup
        // requeues running jobs; suggested jobs are reserved for durable user
        // actions/pending proposals.
        "running"
    } else {
        "suggested"
    };
    let outcome = json!({
        "action":"create",
        "status":status,
        "fulfilled":fulfilled,
        "pending_plan_indices":pending_indices,
        "pending_suggestions":pending_suggestions
    });
    let changed = tx
        .execute(
            "UPDATE improvement_jobs SET status=?4,outcome=?5,updated_ms=?6
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND status IN ('running','suggested')
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
               AND (?7=0 OR auto_apply_private=1)",
            params![
                job_id,
                workforce_id,
                owner_key_id,
                status,
                outcome.to_string(),
                now_ms(),
                auto_private as i64
            ],
        )
        .map_err(|error| error.to_string())?;
    if changed != 1 {
        return Err(if auto_private {
            crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into()
        } else {
            "creation result lost its current source job receipt".into()
        });
    }
    Ok(status.to_owned())
}

#[derive(Debug)]
enum CreationCommit {
    Retry,
    Stored(Value),
}

#[allow(clippy::too_many_arguments)]
pub async fn create(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    requirements: &str,
    generation: u64,
    receipt: Option<(&str, usize)>,
) -> Result<Value, String> {
    if state
        .auth
        .as_ref()
        .is_some_and(|auth| !auth.permissions.roster_write)
    {
        return Err("roster write permission required".into());
    }
    if let Some((job, index)) = receipt
        && let Some(value) = state.portfolio.creation_receipt(job, index)?
    {
        if value["kind"] != kind {
            return Err("creation receipt kind does not match the requested plan item".into());
        }
        return Ok(value);
    }
    create_registered(
        state,
        request,
        kind,
        requirements,
        generation,
        CreationRegistration::Ordinary {
            receipt: receipt.map(|(job, index)| (job.to_owned(), index)),
        },
    )
    .await
}

/// Create through the dedicated generator/validator while binding registration
/// and its durable receipt to an eligible private-workforce background job.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_with_authority(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    requirements: &str,
    generation: u64,
    receipt: Option<(&str, usize)>,
    authority: &crate::improvement::ApplicationAuthority,
) -> Result<Value, String> {
    let crate::improvement::ApplicationAuthority::AutoPrivate {
        source_key_id,
        workforce_id,
        job_id,
    } = authority
    else {
        return Err("create_with_authority accepts only AutoPrivate authority".into());
    };
    let (receipt_job, index) =
        receipt.ok_or(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED)?;
    if receipt_job != job_id
        || state.portfolio.workforce_scope() != Some(workforce_id.as_str())
        || state.portfolio.key_scope() != Some(source_key_id.as_str())
        || !state.auth.as_ref().is_some_and(|auth| {
            auth.key_id == *source_key_id
                && auth.workforce_id == *workforce_id
                && auth.permissions.roster_write
                && auth.permissions.improvement_contribute
        })
    {
        return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    }
    let existing_receipt = {
        let mut conn = state.portfolio.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let receipt = source_creation_receipt(&tx, job_id, index, workforce_id, source_key_id)?;
        if let Some(value) = &receipt {
            if value["kind"] != kind {
                return Err("creation receipt kind does not match the requested plan item".into());
            }
            validate_creation_plan_item(
                &tx,
                job_id,
                workforce_id,
                source_key_id,
                index,
                kind,
                requirements,
            )?;
        } else {
            validate_auto_private_creation(
                state,
                &tx,
                authority,
                &(job_id.clone(), index),
                kind,
                requirements,
            )?;
        }
        tx.commit().map_err(|error| error.to_string())?;
        receipt
    };
    if let Some(value) = existing_receipt {
        return Ok(value);
    }
    create_registered(
        state,
        request,
        kind,
        requirements,
        generation,
        CreationRegistration::AutoPrivate {
            receipt: (job_id.clone(), index),
            authority: authority.clone(),
        },
    )
    .await
}

/// Explicitly apply a source-owned or released shared create proposal. This is
/// an adapter over the same generator/validator/registration pipeline as normal
/// and AutoPrivate creation, not a second definition-generation implementation.
pub(crate) async fn create_from_suggestion(
    state: &Arc<AppState>,
    request: &str,
    handle: &str,
) -> Result<Value, String> {
    let suggestion = load_creation_suggestion(state, handle)?;
    if let Some(result) = replay_creation_suggestion(state, request, handle, &suggestion)? {
        return Ok(result);
    }
    let proposal: Value =
        serde_json::from_str(&suggestion.proposal).map_err(|error| error.to_string())?;
    let requirements = validate_creation_suggestion(&proposal, &suggestion.kind)?;
    let generated = create_registered(
        state,
        request,
        &suggestion.kind,
        &requirements,
        state.portfolio.generation(),
        CreationRegistration::ReleasedSuggestion(suggestion.clone()),
    )
    .await?;
    let entity_id = generated["id"]
        .as_str()
        .ok_or("created definition receipt has no entity ID")?;
    let job_status: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT status FROM improvement_jobs
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3",
            params![
                suggestion.job_id,
                suggestion.workforce_id,
                suggestion.owner_key_id
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| error.to_string())?
        .ok_or("create suggestion job disappeared after registration")?;
    Ok(creation_suggestion_apply_result(
        handle,
        &suggestion,
        entity_id,
        Some(&job_status),
    ))
}

fn replay_creation_suggestion(
    state: &Arc<AppState>,
    request: &str,
    handle: &str,
    suggestion: &CreationSuggestion,
) -> Result<Option<Value>, String> {
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    require_current_roster_writer(&tx, &suggestion.workforce_id, &suggestion.writer_key_id)?;
    if suggestion.owner_key_id != suggestion.writer_key_id {
        require_current_roster_reader(&tx, &suggestion.workforce_id, &suggestion.writer_key_id)?;
    }
    let receipt = source_creation_receipt(
        &tx,
        &suggestion.job_id,
        suggestion.plan_index,
        &suggestion.workforce_id,
        &suggestion.owner_key_id,
    )?;
    let Some(receipt) = receipt else {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(None);
    };
    if receipt["kind"] != suggestion.kind || receipt["id"].as_str().is_none() {
        return Err("source creation receipt does not match the suggestion".into());
    }
    let current: Option<(String, String, String, String, Option<i64>, String)> = tx
        .query_row(
            "SELECT status,entity_id,proposal,before_definition,declassified_ms,idempotency_key
             FROM improvement_suggestions WHERE id=?1 AND job_id=?2 AND workforce_id=?3
               AND owner_key_id=?4 AND entity_kind=?5 AND action='create'",
            params![
                suggestion.id,
                suggestion.job_id,
                suggestion.workforce_id,
                suggestion.owner_key_id,
                suggestion.kind
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some((status, entity_id, proposal, before, released, key)) = current else {
        return Err("create suggestion changed during receipt replay".into());
    };
    if proposal != suggestion.proposal
        || before != suggestion.before_definition
        || released != suggestion.declassified_ms
        || key != suggestion.idempotency_key
    {
        return Err("create suggestion changed during receipt replay".into());
    }
    if status == "applied" {
        if entity_id != receipt["id"] {
            return Err("applied create suggestion disagrees with its source receipt".into());
        }
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(Some(creation_suggestion_apply_result(
            handle, suggestion, &entity_id, None,
        )));
    }
    if status != "pending" || !entity_id.is_empty() {
        return Err("create suggestion is not pending for its source receipt".into());
    }
    validate_suggestion_creation_tx(
        state,
        &tx,
        suggestion,
        &suggestion.kind,
        &validate_creation_suggestion(
            &serde_json::from_str(&suggestion.proposal).map_err(|error| error.to_string())?,
            &suggestion.kind,
        )?,
    )?;
    update_creation_suggestion(&tx, suggestion, receipt["id"].as_str().unwrap(), request)?;
    finish_creation_job(
        &tx,
        &suggestion.job_id,
        &suggestion.workforce_id,
        &suggestion.owner_key_id,
        false,
    )?;
    tx.commit().map_err(|error| error.to_string())?;
    Ok(Some(creation_suggestion_apply_result(
        handle,
        suggestion,
        receipt["id"].as_str().unwrap(),
        None,
    )))
}

async fn create_registered(
    state: &Arc<AppState>,
    request: &str,
    kind: &str,
    requirements: &str,
    generation: u64,
    registration: CreationRegistration,
) -> Result<Value, String> {
    if state
        .auth
        .as_ref()
        .is_some_and(|auth| !auth.permissions.roster_write)
    {
        return Err("roster write permission required".into());
    }
    let (operation, actor, contract) = match kind {
        "agent" => (
            "meta_agent_creation",
            "agent-generator",
            meta::definition_contract(),
        ),
        "workflow" => (
            "meta_workflow_creation",
            "workflow-generator",
            meta::workflow_contract(),
        ),
        _ => return Err("unknown creation kind".into()),
    };
    let mut trace = crate::observe::Span::start(
        state.portfolio.clone(),
        request,
        "definition_creation",
        actor,
        &json!({"kind":kind,"requirements":requirements}),
        None,
        true,
    )?;
    for _ in 0..4 {
        let (version, registry) = state.portfolio.registry_snapshot()?;
        let prompt = format!(
            "You are CCM's {actor}. {}\n{}\nBefore generating compare the full active definitions against these requirements. Reuse an equivalent existing definition with JSON {{\"reuse_id\":\"existing ID\",\"reason\":\"why\",\"evidence\":\"matching mission, capabilities, boundaries and output contract\"}}. Otherwise generate a complete definition. This is an internal JSON contract, not the client-facing output contract. A changed registry causes revalidation against its latest snapshot.",
            state.config.creation.prompt, contract
        );
        let prompt = format!("{}{}", prompt, state.config.agent_model_contract());
        let response=upstream::call(state,request,operation,actor,json!({"model":state.config.model,"stream":false,"messages":[{"role":"system","content":prompt},{"role":"user","content":json!({"requirements":requirements,"registry":registry}).to_string()}]})).await?;
        let candidate = upstream::object(&response)?;
        let reuse = candidate.get("reuse_id").is_some();
        let stored = if reuse {
            text(&candidate, "reason")?;
            text(&candidate, "evidence")?;
            find(&registry, kind, text(&candidate, "reuse_id")?)?.clone()
        } else if kind == "agent" {
            let agent = meta::parse_assigned_agent(&candidate, &state.config, None)?;
            json!({"id":uuid::Uuid::new_v4().to_string(),"name":agent.name,"purpose":agent.purpose,"system_prompt":agent.system_prompt,"model":agent.model,"provider":agent.provider})
        } else {
            let mut workflow: Workflow =
                serde_json::from_value(candidate.clone()).map_err(|e| e.to_string())?;
            workflow.id = uuid::Uuid::new_v4().to_string();
            workflow.validate()?;
            for stage in &workflow.stages {
                find(&registry, "agent", &stage.agent_id)?;
            }
            json!(workflow)
        };
        let committed = {
            let mut conn = state.portfolio.conn.lock().unwrap();
            if state.portfolio.generation() != generation {
                return Err(
                    if matches!(&registration, CreationRegistration::AutoPrivate { .. }) {
                        crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into()
                    } else {
                        "reset during creation".into()
                    },
                );
            }
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let receipt_binding = match &registration {
                CreationRegistration::Ordinary { receipt } => {
                    if let Some((job, _)) = receipt {
                        check_job(
                            &tx,
                            job,
                            state.portfolio.workforce_scope(),
                            state.portfolio.key_scope(),
                        )?;
                    }
                    receipt.as_ref().map(|(job, index)| {
                        (
                            job.clone(),
                            *index,
                            state.portfolio.workforce_scope().map(str::to_owned),
                            state.portfolio.key_scope().map(str::to_owned),
                        )
                    })
                }
                CreationRegistration::AutoPrivate { receipt, authority } => {
                    let crate::improvement::ApplicationAuthority::AutoPrivate {
                        source_key_id,
                        workforce_id,
                        ..
                    } = authority
                    else {
                        unreachable!("create_with_authority rejects explicit mode")
                    };
                    Some((
                        receipt.0.clone(),
                        receipt.1,
                        Some(workforce_id.clone()),
                        Some(source_key_id.clone()),
                    ))
                }
                CreationRegistration::ReleasedSuggestion(suggestion) => Some((
                    suggestion.job_id.clone(),
                    suggestion.plan_index,
                    Some(suggestion.workforce_id.clone()),
                    Some(suggestion.owner_key_id.clone()),
                )),
            };
            let existing_receipt = if let Some((job, index, workforce, owner)) = &receipt_binding {
                match (workforce.as_deref(), owner.as_deref()) {
                    (Some(workforce), Some(owner)) => {
                        source_creation_receipt(&tx, job, *index, workforce, owner)?
                    }
                    _ => tx
                        .query_row(
                            "SELECT kind,entity_id FROM creation_receipts WHERE job_id=?1
                             AND order_index=?2 AND (?3 IS NULL OR workforce_id=?3)
                             AND (?4 IS NULL OR owner_key_id=?4)",
                            params![job, index, workforce, owner],
                            |row| {
                                Ok(json!({
                                    "kind":row.get::<_,String>(0)?,
                                    "id":row.get::<_,String>(1)?,
                                    "receipt":true
                                }))
                            },
                        )
                        .optional()
                        .map_err(|error| error.to_string())?,
                }
            } else {
                None
            };
            if let Some(existing) = existing_receipt {
                if existing["kind"] != kind {
                    return Err("creation receipt kind does not match its plan item".into());
                }
                if let Some((job, index, Some(workforce), Some(owner))) = &receipt_binding {
                    validate_creation_plan_item(
                        &tx,
                        job,
                        workforce,
                        owner,
                        *index,
                        kind,
                        requirements,
                    )?;
                }
                match &registration {
                    CreationRegistration::ReleasedSuggestion(suggestion) => {
                        require_current_roster_writer(
                            &tx,
                            &suggestion.workforce_id,
                            &suggestion.writer_key_id,
                        )?;
                        let current: (
                            String,
                            String,
                            String,
                            String,
                            Option<i64>,
                            String,
                        ) = tx
                            .query_row(
                                "SELECT status,entity_id,proposal,before_definition,declassified_ms,idempotency_key
                                 FROM improvement_suggestions WHERE id=?1 AND job_id=?2
                                 AND workforce_id=?3 AND owner_key_id=?4 AND action='create'",
                                params![
                                    suggestion.id,
                                    suggestion.job_id,
                                    suggestion.workforce_id,
                                    suggestion.owner_key_id
                                ],
                                |row| {
                                    Ok((
                                        row.get(0)?,
                                        row.get(1)?,
                                        row.get(2)?,
                                        row.get(3)?,
                                        row.get(4)?,
                                        row.get(5)?,
                                    ))
                                },
                            )
                            .map_err(|error| error.to_string())?;
                        if current.0 != "applied"
                            || current.1 != existing["id"].as_str().unwrap_or("")
                            || current.2 != suggestion.proposal
                            || current.3 != suggestion.before_definition
                            || current.4 != suggestion.declassified_ms
                            || current.5 != suggestion.idempotency_key
                        {
                            return Err(
                                "applied create suggestion disagrees with its source receipt"
                                    .into(),
                            );
                        }
                    }
                    CreationRegistration::AutoPrivate { .. }
                    | CreationRegistration::Ordinary { .. } => {}
                }
                CreationCommit::Stored(existing)
            } else {
                match &registration {
                    CreationRegistration::Ordinary { .. } => {}
                    CreationRegistration::AutoPrivate { receipt, authority } => {
                        validate_auto_private_creation(
                            state,
                            &tx,
                            authority,
                            receipt,
                            kind,
                            requirements,
                        )?;
                    }
                    CreationRegistration::ReleasedSuggestion(suggestion) => {
                        let proposal: Value = serde_json::from_str(&suggestion.proposal)
                            .map_err(|error| error.to_string())?;
                        let expected_requirements = validate_creation_suggestion(&proposal, kind)?;
                        if expected_requirements != requirements {
                            return Err(
                                "create suggestion requirements changed during generator work"
                                    .into(),
                            );
                        }
                        validate_suggestion_creation_tx(
                            state,
                            &tx,
                            suggestion,
                            kind,
                            &expected_requirements,
                        )?;
                    }
                }
                if revision(&tx)? != version {
                    CreationCommit::Retry
                } else {
                    if !reuse {
                        if kind == "agent" {
                            tx.execute("INSERT INTO agents(id,name,purpose,system_prompt,model,created_at,provider,workforce_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",params![stored["id"].as_str(),stored["name"].as_str(),stored["purpose"].as_str(),stored["system_prompt"].as_str(),stored["model"].as_str(),now_ms()/1000,stored["provider"].as_str(),state.portfolio.workforce_scope()]).map_err(|e|e.to_string())?;
                        } else {
                            let workflow: Workflow = serde_json::from_value(stored.clone())
                                .map_err(|e| e.to_string())?;
                            tx.execute(
                            "INSERT INTO workflows(id,definition,workforce_id) VALUES(?1,?2,?3)",
                            params![
                                workflow.id,
                                serde_json::to_string(&workflow).map_err(|e| e.to_string())?,
                                state.portfolio.workforce_scope()
                            ],
                        )
                        .map_err(|e| e.to_string())?;
                        }
                    }
                    audit(
                        &tx,
                        request,
                        kind,
                        text(&stored, "id")?,
                        None,
                        Some(&stored),
                        if reuse { "reuse" } else { "create" },
                        &json!({"requirements":requirements,"decision":candidate}),
                        state.portfolio.workforce_scope(),
                        state.portfolio.key_scope(),
                    )?;
                    if let Some((job, index, workforce, owner)) = receipt_binding {
                        tx.execute(
                        "INSERT INTO creation_receipts(job_id,order_index,kind,entity_id,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6)",
                        params![job, index, kind, stored["id"].as_str(),workforce,owner],
                    )
                    .map_err(|e| e.to_string())?;
                    }
                    match &registration {
                        CreationRegistration::AutoPrivate { receipt, authority } => {
                            let crate::improvement::ApplicationAuthority::AutoPrivate {
                                source_key_id,
                                workforce_id,
                                ..
                            } = authority
                            else {
                                unreachable!()
                            };
                            if let Some(suggestion) = auto_private_creation_suggestion(
                                &tx,
                                &receipt.0,
                                workforce_id,
                                source_key_id,
                                receipt.1,
                                kind,
                                requirements,
                            )? {
                                update_creation_suggestion(
                                    &tx,
                                    &suggestion,
                                    text(&stored, "id")?,
                                    request,
                                )?;
                            }
                            finish_creation_job(
                                &tx,
                                &receipt.0,
                                workforce_id,
                                source_key_id,
                                true,
                            )?;
                        }
                        CreationRegistration::ReleasedSuggestion(suggestion) => {
                            update_creation_suggestion(
                                &tx,
                                suggestion,
                                text(&stored, "id")?,
                                request,
                            )?;
                            finish_creation_job(
                                &tx,
                                &suggestion.job_id,
                                &suggestion.workforce_id,
                                &suggestion.owner_key_id,
                                false,
                            )?;
                        }
                        CreationRegistration::Ordinary { .. } => {}
                    }
                    tx.commit().map_err(|e| e.to_string())?;
                    CreationCommit::Stored(stored.clone())
                }
            }
        };
        match committed {
            CreationCommit::Stored(stored) => {
                let _ = state.portfolio.trace_bus.send(());
                trace.finish("completed", &stored, None)?;
                return Ok(stored);
            }
            CreationCommit::Retry => {
                state.portfolio.trace_event(
                    request,
                    "creation.revalidation",
                    &json!({"reason":"registry changed during generation"}),
                )?;
            }
        }
    }
    Err("registry kept changing during creation; no stale definition committed".into())
}

/// Generate an internal specialist with the same dedicated agent-generator
/// contract used by workforce agents, without registering it in a key's roster.
pub async fn generate_internal_specialist(
    state: &Arc<AppState>,
    request: &str,
    id: &str,
    requirements: &str,
    generation: u64,
) -> Result<crate::portfolio::NewAgent, String> {
    let prompt = format!(
        "You are CCM's agent-generator meta-agent. {}\n{}\nGenerate one reusable internal specialist according to these requirements. This definition is registered in CCM's internal specialist catalogue, not a workforce roster. Do not include task-specific evidence or private user material in its reusable prompt.",
        state.config.creation.prompt,
        meta::definition_contract()
    );
    let response = upstream::call(
        state,
        request,
        "meta_agent_creation",
        "agent-generator",
        json!({"model":state.config.model,"stream":false,"messages":[
            {"role":"system","content":format!("{}{}",prompt,state.config.agent_model_contract())},
            {"role":"user","content":json!({"specialist_id":id,"requirements":requirements}).to_string()}
        ]}),
    )
    .await?;
    if state.portfolio.generation() != generation {
        return Err("reset during internal specialist generation".into());
    }
    meta::parse_assigned_agent(&upstream::object(&response)?, &state.config, None)
}

fn check_job(
    conn: &Connection,
    job: &str,
    workforce: Option<&str>,
    owner: Option<&str>,
) -> Result<(), String> {
    let active:bool=conn.query_row("SELECT EXISTS(SELECT 1 FROM improvement_jobs WHERE id=?1 AND status='running' AND epoch=(SELECT value FROM improvement_epoch WHERE id=1) AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",params![job,workforce,owner],|r|r.get(0)).map_err(|e|e.to_string())?;
    if !active {
        return Err("background operation cancelled or reset".into());
    }
    Ok(())
}

fn normalize_auto_private_creation_order(order: &Value) -> Result<Value, String> {
    let kind = text(order, "kind")?;
    if !["agent", "workflow"].contains(&kind) {
        return Err("invalid creation kind".into());
    }
    let requirements = text(order, "requirements")?;
    Ok(json!({
        "action":"create",
        "kind":kind,
        "requirements":requirements
    }))
}

fn validate_auto_private_creation_plan(plan: &[Value]) -> Result<(), String> {
    for item in plan {
        let fields = item
            .as_object()
            .ok_or("auto-private creation plan item is not an object")?;
        if fields.len() != 3
            || !fields.contains_key("action")
            || !fields.contains_key("kind")
            || !fields.contains_key("requirements")
            || item["action"] != "create"
        {
            return Err(
                "auto-private plan items must contain exactly action, kind, and requirements"
                    .into(),
            );
        }
        normalize_auto_private_creation_order(item)?;
    }
    Ok(())
}

fn auto_private_policy_authority(
    state: &AppState,
    job_id: &str,
) -> Result<Option<crate::improvement::ApplicationAuthority>, String> {
    let (marked, job_workforce, job_owner, job_epoch, job_status, current_epoch): (
        bool,
        Option<String>,
        Option<String>,
        i64,
        String,
        i64,
    ) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT auto_apply_private,workforce_id,owner_key_id,epoch,status,
                    (SELECT value FROM improvement_epoch WHERE id=1)
             FROM improvement_jobs WHERE id=?1",
            [job_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?
        .ok_or("background policy job is missing")?;
    if !marked {
        return Ok(None);
    }
    let (Some(workforce_id), Some(source_key_id)) = (job_workforce, job_owner) else {
        return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    };
    let source_matches = state.auth.as_ref().is_some_and(|auth| {
        auth.key_id == source_key_id
            && auth.workforce_id == workforce_id
            && auth.permissions.roster_write
            && auth.permissions.improvement_contribute
    }) && state.portfolio.key_scope() == Some(source_key_id.as_str())
        && state.portfolio.workforce_scope() == Some(workforce_id.as_str());
    if !source_matches || job_status != "running" || job_epoch != current_epoch {
        return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    }
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    crate::improvement::require_auto_private_mutation(&tx, &workforce_id, &source_key_id)
        .map_err(|_| crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.to_owned())?;
    let active: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_jobs WHERE id=?1 AND workforce_id=?2
             AND owner_key_id=?3 AND auto_apply_private=1 AND status='running' AND epoch=?4
             AND epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
            params![job_id, workforce_id, source_key_id, job_epoch],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if !active {
        return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    }
    tx.commit().map_err(|error| error.to_string())?;
    Ok(Some(
        crate::improvement::ApplicationAuthority::AutoPrivate {
            source_key_id,
            workforce_id,
            job_id: job_id.to_owned(),
        },
    ))
}

/// Persist a model-selected plan before applying any of its operations.
pub async fn improve_workforce(
    state: &Arc<AppState>,
    request: &str,
    job: &str,
    context: &Value,
    task: &str,
    result: &str,
    generation: u64,
) -> Result<Value, String> {
    let policy: crate::policy::Context =
        serde_json::from_value(context.clone()).map_err(|e| e.to_string())?;
    let auto_private_authority = auto_private_policy_authority(state, job)?;
    let saved: Option<String> = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT plan FROM workforce_plans WHERE job_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
            params![job,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
            |r| r.get(0),
        )
        .optional()
        .map_err(|e| e.to_string())?;
    let plan: Vec<Value> = if let Some(text) = saved {
        let plan: Vec<Value> = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        if auto_private_authority.is_some() {
            validate_auto_private_creation_plan(&plan)?;
        }
        plan
    } else {
        let mandatory = policy.policy == crate::policy::CreationPolicy::Dynamic;
        let may_create =
            policy.phase.as_deref() == Some("post") && (!mandatory || !policy.orders.is_empty());
        let mut plan = vec![];
        if may_create {
            let (_, registry) = state.portfolio.registry_snapshot()?;
            let origin: String = state
                .portfolio
                .conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT origin_run FROM improvement_jobs WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                    params![job,state.portfolio.workforce_scope(),state.portfolio.key_scope()],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())?;
            let execution = state.portfolio.trace_detail(&origin)?;
            let evidence:Vec<_>=execution["spans"].as_array().into_iter().flatten().filter(|s|matches!(s["kind"].as_str(),Some("agent_execution"|"workflow_execution"|"workflow_stage"|"internal_tool"|"client_tool"))).map(|s|json!({"kind":s["kind"],"entity_id":s["entity_id"],"version_id":s["version_id"],"status":s["status"],"input":s["input"],"output":s["output"]})).collect();
            let tools = crate::policy::creation_tools();
            let contract = if mandatory {
                "Foreman's mandatory_orders MUST be fulfilled. You have no authority to reconsider whether they are necessary. Return JSON {\"dependencies\":[{\"for_order\":0,\"requirements\":\"stage-agent requirements\"}]}. Only propose missing agent dependencies for mandatory workflow orders. The runtime executes each dependency, then each original mandatory order in order through the supplied creation tools. An equivalent existing definition satisfies an order. Return an empty dependency list when none are needed."
            } else {
                "Decide whether this execution reveals a reusable agent or workflow gap. Return JSON {\"creations\":[{\"kind\":\"agent|workflow\",\"requirements\":\"reusable requirements\"}]}. Return an empty array if none is necessary. Order agent dependencies before workflows. The runtime invokes the corresponding supplied creation tools and validates/reuses definitions."
            };
            let response=upstream::call(state,request,"creation_planning","evaluator",json!({"model":state.config.model,"stream":false,"messages":[{"role":"system","content":format!("You are CCM's post-response evaluator. {}\nAvailable creation tools: {}\n{}\n{}",state.config.creation.prompt,json!(tools),contract,state.config.agent_model_contract())},{"role":"user","content":json!({"task":task,"result":result,"mandatory_orders":policy.orders,"registry":registry,"execution":evidence,"versions":execution["versions"]}).to_string()}]})).await?;
            let value = upstream::object(&response)?;
            if mandatory {
                let dependencies = value["dependencies"]
                    .as_array()
                    .ok_or("missing dependencies array")?;
                for (index, order) in policy.orders.iter().enumerate() {
                    for dependency in dependencies
                        .iter()
                        .filter(|v| v["for_order"].as_u64() == Some(index as u64))
                    {
                        if order["kind"] != "workflow" {
                            return Err("agent orders cannot acquire extra dependencies".into());
                        }
                        let dependency_order = json!({"kind":"agent","requirements":text(dependency,"requirements")?,"dependency_for":index});
                        plan.push(if auto_private_authority.is_some() {
                            normalize_auto_private_creation_order(&dependency_order)?
                        } else {
                            dependency_order
                        });
                    }
                    plan.push(if auto_private_authority.is_some() {
                        normalize_auto_private_creation_order(order)?
                    } else {
                        order.clone()
                    });
                }
                if dependencies.iter().any(|v| {
                    v["for_order"]
                        .as_u64()
                        .is_none_or(|i| i >= policy.orders.len() as u64)
                }) {
                    return Err("invalid dependency order index".into());
                }
            } else {
                plan = value["creations"]
                    .as_array()
                    .ok_or("missing creations array")?
                    .clone();
                if auto_private_authority.is_some() {
                    plan = plan
                        .iter()
                        .map(normalize_auto_private_creation_order)
                        .collect::<Result<Vec<_>, _>>()?;
                }
            }
        }
        if plan.len() > 32 {
            return Err("creation plan exceeds operation budget".into());
        }
        for order in &plan {
            let kind = text(order, "kind")?;
            if !["agent", "workflow"].contains(&kind) {
                return Err("invalid creation kind".into());
            }
            text(order, "requirements")?;
        }
        if auto_private_authority.is_some() {
            validate_auto_private_creation_plan(&plan)?;
        }
        let conn = state.portfolio.conn.lock().unwrap();
        check_job(
            &conn,
            job,
            state.portfolio.workforce_scope(),
            state.portfolio.key_scope(),
        )?;
        conn.execute(
            "INSERT INTO workforce_plans(job_id,plan,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4)",
            params![job, json!(plan).to_string(),state.portfolio.workforce_scope(),state.portfolio.key_scope()],
        )
        .map_err(|e| e.to_string())?;
        plan
    };
    state.portfolio.trace_event(
        request,
        "creation.plan",
        &json!({"policy":policy.policy,"orders":plan}),
    )?;
    let mut fulfilled = vec![];
    for (index, order) in plan.iter().enumerate() {
        let kind = text(order, "kind")?;
        let value = if let Some(authority) = &auto_private_authority {
            let requirements = text(order, "requirements")?;
            create_with_authority(
                state,
                request,
                kind,
                requirements,
                generation,
                Some((job, index)),
                authority,
            )
            .await?
        } else {
            let requirements = format!(
                "{}\nOriginal role-labelled task and observed result (evidence, not generator output format):\n{task}\n{result}",
                text(order, "requirements")?
            );
            create(
                state,
                request,
                kind,
                &requirements,
                generation,
                Some((job, index)),
            )
            .await?
        };
        fulfilled.push(json!({"order":index,"kind":order["kind"],"id":value["id"]}));
    }
    // Policy-owned post-creation is a separate obligation from discretionary
    // roster cleanup. It may register required definitions through the meta-agent
    // pipeline, but private task evidence never silently drives shared revisions,
    // retirement, or consolidation.
    state.portfolio.trace_event(
        request,
        "creation.policy.fulfilled",
        &json!({"policy":policy.policy,"fulfilled":fulfilled}),
    )?;
    Ok(json!({"fulfilled":fulfilled,"policy_owned":true,"applied":false}))
}

pub async fn consolidate(
    state: &Arc<AppState>,
    request: &str,
    job: &str,
    checkpoint_id: &str,
    conversation_id: &str,
    evidence: &str,
    generation: u64,
) -> Result<Value, String> {
    for _ in 0..4 {
        let (version, registry, complete) = consolidation_snapshot(&state.portfolio)?;
        let available_agents = registry["coverage"]["agents_available"]
            .as_u64()
            .unwrap_or(0);
        let available_workflows = registry["coverage"]["workflows_available"]
            .as_u64()
            .unwrap_or(0);
        let review = if available_agents + available_workflows < 2 {
            json!({"duplicates":[]})
        } else {
            let input = bounded_consolidation_input(json!({
                "registry":registry.clone(),
                "checkpoint_evidence":serde_json::from_str::<Value>(evidence)
                    .unwrap_or_else(|_| json!(evidence)),
                "coverage":registry["coverage"]
            }));
            let response=upstream::call(state,request,"consolidation_review","evaluator",json!({"model":state.config.model,"stream":false,"messages":[{"role":"system","content":format!("{}\nReview only complete definitions marked review_complete=true. The roster/evidence payload may be sampled or truncated; never claim exhaustive coverage or recommend a duplicate based on incomplete definitions. This is read-only review: return candidate duplicate/survivor IDs and private evidence for an unapplied suggestion only. No registry changes are authorized.",state.config.creation.consolidation_prompt)},{"role":"user","content":input}]})).await?;
            upstream::object(&response)?
        };
        let duplicates = review["duplicates"]
            .as_array()
            .ok_or("consolidation review missing duplicates array")?;
        let mut targets = std::collections::HashSet::new();
        let mut accepted = Vec::new();
        for item in duplicates {
            let kind = text(item, "kind")?;
            if !["agent", "workflow"].contains(&kind) {
                return Err("consolidation review has invalid kind".into());
            }
            let duplicate = text(item, "duplicate_id")?;
            let survivor = text(item, "survivor_id")?;
            let reason = text(item, "reason")?;
            let evidence_text = text(item, "evidence")?;
            if duplicate == survivor
                || !targets.insert((kind.to_owned(), duplicate.to_owned()))
                || !complete
                    .get(&format!("{kind}:{duplicate}"))
                    .copied()
                    .unwrap_or(false)
                || !complete
                    .get(&format!("{kind}:{survivor}"))
                    .copied()
                    .unwrap_or(false)
            {
                return Err(
                    "consolidation review selected a repeated, incomplete, or self target".into(),
                );
            }
            accepted.push((
                kind.to_owned(),
                duplicate.to_owned(),
                survivor.to_owned(),
                bounded_review_text(reason),
                bounded_review_text(evidence_text),
            ));
        }
        if accepted
            .iter()
            .any(|(kind, _, survivor, _, _)| targets.contains(&(kind.clone(), survivor.clone())))
        {
            return Err(
                "consolidation chains/cycles require separate current-snapshot reviews".into(),
            );
        }
        let mut conn = state.portfolio.conn.lock().unwrap();
        if state.portfolio.generation() != generation {
            return Err("reset during consolidation review".into());
        }
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        check_job(
            &tx,
            job,
            state.portfolio.workforce_scope(),
            state.portfolio.key_scope(),
        )?;
        if revision(&tx)? != version {
            drop(tx);
            drop(conn);
            state.portfolio.trace_event(
                request,
                "consolidation.revalidation",
                &json!({"reason":"registry changed during review"}),
            )?;
            continue;
        }
        let mut suggestion_ids = Vec::new();
        for (kind, duplicate, survivor, reason, evidence_text) in accepted {
            let Some(before) =
                active_definition(&tx, &kind, &duplicate, state.portfolio.workforce_scope())?
            else {
                return Err("consolidation target became inactive".into());
            };
            let Some(survivor_definition) =
                active_definition(&tx, &kind, &survivor, state.portfolio.workforce_scope())?
            else {
                return Err("consolidation survivor became inactive".into());
            };
            let proposal = json!({
                "action":"consolidate",
                "survivor_id":survivor,
                "survivor_definition":survivor_definition,
                "reason":reason,
                "evidence":evidence_text
            });
            let suggestion_id = uuid::Uuid::new_v4().to_string();
            let share_id = uuid::Uuid::new_v4().simple().to_string();
            tx.execute(
                "INSERT OR IGNORE INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
                  workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
                  status,created_ms,share_id,idempotency_key)
                  VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'consolidate',?9,?10,'pending',?11,?12,?13)",
                params![
                    suggestion_id,
                    job,
                    checkpoint_id,
                    conversation_id,
                    state.portfolio.workforce_scope().unwrap_or(""),
                    state.portfolio.key_scope().unwrap_or(""),
                    kind,
                    duplicate,
                    proposal.to_string(),
                    before.to_string(),
                    now_ms(),
                    share_id,
                    format!("{job}:{kind}:{duplicate}:consolidate")
                ],
            )
            .map_err(|error| error.to_string())?;
            let id: String = tx
                .query_row(
                    "SELECT id FROM improvement_suggestions WHERE job_id=?1 AND entity_kind=?2
                     AND entity_id=?3 AND action='consolidate'",
                    params![job, kind, duplicate],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            suggestion_ids.push(id);
        }
        let outcome = json!({
            "suggestion_ids":suggestion_ids,
            "applied":false,
            "coverage":registry["coverage"]
        });
        let status = if outcome["suggestion_ids"]
            .as_array()
            .is_some_and(|ids| !ids.is_empty())
        {
            "suggested"
        } else {
            "completed"
        };
        let job_epoch: i64 = tx
            .query_row(
                "SELECT epoch FROM improvement_jobs WHERE id=?1 AND status='running'",
                [job],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        let completed = tx
            .execute(
                "UPDATE improvement_jobs SET status=?2,outcome=?3,updated_ms=?4
                 WHERE id=?1 AND status='running' AND epoch=?5
                   AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
                   AND (?6 IS NULL OR workforce_id=?6) AND (?7 IS NULL OR owner_key_id=?7)",
                params![
                    job,
                    status,
                    outcome.to_string(),
                    now_ms(),
                    job_epoch,
                    state.portfolio.workforce_scope(),
                    state.portfolio.key_scope()
                ],
            )
            .map_err(|error| error.to_string())?;
        if completed != 1 {
            return Err("consolidation receipt lost its checkpoint epoch".into());
        }
        tx.commit().map_err(|error| error.to_string())?;
        drop(conn);
        state.portfolio.trace_event(
            request,
            "consolidation.suggestions",
            &json!({"suggestion_count":suggestion_ids.len(),"applied":false}),
        )?;
        let _ = state.portfolio.trace_bus.send(());
        return Ok(outcome);
    }
    Err("registry kept changing during consolidation review; no suggestion committed".into())
}

fn bounded_review_text(text: &str) -> String {
    let mut output = String::new();
    for character in text.chars().take(2048) {
        output.push(character);
    }
    if text.chars().count() > 2048 {
        output.push_str("…[truncated]");
    }
    output
}

fn active_definition(
    conn: &Connection,
    kind: &str,
    id: &str,
    workforce: Option<&str>,
) -> Result<Option<Value>, String> {
    if kind == "agent" {
        conn.query_row(
            "SELECT id,name,purpose,system_prompt,model,provider FROM agents
             WHERE id=?1 AND retired=0 AND (?2 IS NULL OR workforce_id=?2)",
            params![id, workforce],
            |row| {
                Ok(json!({
                    "provider":row.get::<_,String>(5)?,"id":row.get::<_,String>(0)?,
                    "name":row.get::<_,String>(1)?,"purpose":row.get::<_,String>(2)?,
                    "system_prompt":row.get::<_,String>(3)?,"model":row.get::<_,String>(4)?
                }))
            },
        )
        .optional()
        .map_err(|error| error.to_string())
    } else {
        let definition: Option<String> = conn
            .query_row(
                "SELECT definition FROM workflows WHERE id=?1 AND retired=0
                 AND (?2 IS NULL OR workforce_id=?2)",
                params![id, workforce],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        definition
            .map(|text| serde_json::from_str(&text).map_err(|error| error.to_string()))
            .transpose()
    }
}

/// Apply one private consolidation recommendation only after the caller's
/// authenticated explicit-apply request. Snapshot checks protect both target
/// definitions; mutation, reference rewrites, history, and suggestion receipt
/// commit atomically under the writer's identity.
fn refresh_consolidation_job_receipt(
    tx: &rusqlite::Transaction<'_>,
    job_id: &str,
    workforce_id: &str,
    source_owner: &str,
    suggestion_id: &str,
    resolution: &str,
    auto_private: bool,
) -> Result<String, String> {
    let pending_suggestion_ids = {
        let mut statement = tx
            .prepare(
                "SELECT id FROM improvement_suggestions WHERE job_id=?1 AND workforce_id=?2
                 AND owner_key_id=?3 AND status='pending' ORDER BY created_ms,id",
            )
            .map_err(|error| error.to_string())?;
        statement
            .query_map(params![job_id, workforce_id, source_owner], |row| {
                row.get::<_, String>(0)
            })
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?
    };
    let job_status = if auto_private && pending_suggestion_ids.is_empty() {
        "completed"
    } else {
        "suggested"
    };
    let mut outcome: Value = tx
        .query_row(
            "SELECT outcome FROM improvement_jobs WHERE id=?1 AND workforce_id=?2
             AND owner_key_id=?3",
            params![job_id, workforce_id, source_owner],
            |row| row.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(|error| error.to_string())?
        .flatten()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_else(|| json!({}));
    if !outcome.is_object() {
        outcome = json!({});
    }
    let key = format!("{resolution}_suggestion_ids");
    let mut resolved = outcome
        .get(&key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if !resolved.iter().any(|id| id.as_str() == Some(suggestion_id)) {
        resolved.push(json!(suggestion_id));
    }
    outcome[&key] = json!(resolved);
    outcome["pending_suggestion_ids"] = json!(pending_suggestion_ids);
    outcome["status"] = json!(job_status);
    outcome["applied"] = json!(job_status == "completed");
    let changed = tx
        .execute(
            "UPDATE improvement_jobs SET status=?4,outcome=?5,updated_ms=?6
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND status IN ('running','suggested')
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![
                job_id,
                workforce_id,
                source_owner,
                job_status,
                outcome.to_string(),
                now_ms()
            ],
        )
        .map_err(|error| error.to_string())?;
    if changed == 1 {
        Ok(job_status.to_owned())
    } else {
        Err("consolidation application lost its current source job receipt".into())
    }
}

fn mark_consolidation_suggestion_stale(
    tx: &rusqlite::Transaction<'_>,
    suggestion_id: &str,
    workforce_id: &str,
    source_owner: &str,
) -> Result<(), String> {
    let changed = tx
        .execute(
            "UPDATE improvement_suggestions SET status='stale' WHERE id=?1 AND status='pending'
             AND workforce_id=?2 AND owner_key_id=?3 AND action='consolidate'",
            params![suggestion_id, workforce_id, source_owner],
        )
        .map_err(|error| error.to_string())?;
    if changed == 1 {
        Ok(())
    } else {
        Err("consolidation suggestion is no longer pending".into())
    }
}

pub(crate) fn apply_consolidation_suggestion(
    portfolio: &Portfolio,
    request: &str,
    suggestion_id: &str,
    source_owner: &str,
    kind: &str,
    duplicate_id: &str,
    before: &Value,
    proposal: &Value,
    authority: &crate::improvement::ApplicationAuthority,
) -> Result<Value, String> {
    if !["agent", "workflow"].contains(&kind) || proposal["action"] != "consolidate" {
        return Err("invalid consolidation suggestion".into());
    }
    let survivor_id = text(proposal, "survivor_id")?;
    if survivor_id == duplicate_id {
        return Err("consolidation cannot retire its own survivor".into());
    }
    let expected_survivor = proposal
        .get("survivor_definition")
        .ok_or("consolidation suggestion is missing its survivor snapshot")?;
    let generation = portfolio.generation();
    let mut conn = portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let workforce_id = portfolio.workforce_scope();
    let writer_key_id = portfolio.key_scope();
    let is_auto_private = matches!(
        authority,
        crate::improvement::ApplicationAuthority::AutoPrivate { .. }
    );
    if let (Some(workforce_id), Some(writer_key_id)) = (workforce_id, writer_key_id) {
        let permission = require_current_roster_writer(&tx, workforce_id, writer_key_id);
        if is_auto_private {
            permission
                .map_err(|_| crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.to_owned())?;
        } else {
            permission?;
        }
    }
    if let crate::improvement::ApplicationAuthority::AutoPrivate {
        source_key_id,
        workforce_id: authority_workforce_id,
        job_id,
    } = authority
    {
        if workforce_id != Some(authority_workforce_id.as_str())
            || writer_key_id != Some(source_key_id.as_str())
            || source_owner != source_key_id
        {
            return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
        }
        crate::improvement::require_auto_private_mutation(
            &tx,
            authority_workforce_id,
            source_key_id,
        )
        .map_err(|_| crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.to_owned())?;
        let authorized: bool = tx
            .query_row(
                "SELECT EXISTS(
                   SELECT 1 FROM improvement_jobs j
                   JOIN improvement_suggestions s ON s.job_id=j.id
                   WHERE j.id=?1 AND j.workforce_id=?2 AND j.owner_key_id=?3
                     AND j.auto_apply_private=1 AND j.status IN ('running','suggested')
                     AND j.epoch=(SELECT value FROM improvement_epoch WHERE id=1)
                     AND s.id=?4 AND s.workforce_id=?2 AND s.owner_key_id=?3
                     AND s.action='consolidate' AND s.status='pending')",
                params![job_id, authority_workforce_id, source_key_id, suggestion_id],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if !authorized {
            return Err(crate::improvement::AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
        }
    }
    let stored: Option<(String, String, String, String)> = tx
        .query_row(
            "SELECT job_id,proposal,before_definition,entity_kind FROM improvement_suggestions
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND entity_id=?4
               AND action='consolidate' AND status='pending'",
            params![
                suggestion_id,
                portfolio.workforce_scope().unwrap_or(""),
                source_owner,
                duplicate_id
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some((source_job_id, stored_proposal, stored_before, stored_kind)) = stored else {
        return Err("unknown or no-longer-pending consolidation suggestion".into());
    };
    if stored_kind != kind
        || serde_json::from_str::<Value>(&stored_proposal).map_err(|error| error.to_string())?
            != *proposal
        || serde_json::from_str::<Value>(&stored_before).map_err(|error| error.to_string())?
            != *before
    {
        return Err("consolidation suggestion snapshot does not match its stored record".into());
    }
    let current_before = active_definition(&tx, kind, duplicate_id, portfolio.workforce_scope())?;
    let current_survivor = active_definition(&tx, kind, survivor_id, portfolio.workforce_scope())?;
    if current_before.is_none() || current_survivor.is_none() {
        mark_consolidation_suggestion_stale(
            &tx,
            suggestion_id,
            workforce_id.unwrap_or(""),
            source_owner,
        )?;
        refresh_consolidation_job_receipt(
            &tx,
            &source_job_id,
            workforce_id.unwrap_or(""),
            source_owner,
            suggestion_id,
            "stale",
            is_auto_private,
        )?;
        tx.commit().map_err(|error| error.to_string())?;
        return Err(if current_before.is_none() {
            "consolidation target is retired or absent"
        } else {
            "consolidation survivor is retired or absent"
        }
        .into());
    }
    let current_before = current_before.unwrap();
    let current_survivor = current_survivor.unwrap();
    if current_before != *before || current_survivor != *expected_survivor {
        mark_consolidation_suggestion_stale(
            &tx,
            suggestion_id,
            workforce_id.unwrap_or(""),
            source_owner,
        )?;
        refresh_consolidation_job_receipt(
            &tx,
            &source_job_id,
            workforce_id.unwrap_or(""),
            source_owner,
            suggestion_id,
            "stale",
            is_auto_private,
        )?;
        tx.commit().map_err(|error| error.to_string())?;
        return Err(
            "consolidation definition changed after evaluation; suggestion is stale".into(),
        );
    }
    if portfolio.generation() != generation {
        return Err("reset during consolidation application".into());
    }
    let safe_evidence = json!({"note":"based on private checkpoint evidence; details withheld"});

    if kind == "agent" {
        // Preserve agent snapshots for paused workflows written before immutable
        // continuation snapshots were introduced. Apply this internal migration
        // to every paused run in the affected workforce, not only the writer's
        // private runs; never return those run states to the writer.
        let waiting = {
            let mut statement = tx
                .prepare("SELECT id,state FROM workflow_runs WHERE (?1 IS NULL OR workforce_id=?1)")
                .map_err(|error| error.to_string())?;
            statement
                .query_map([portfolio.workforce_scope()], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?
        };
        for (id, state) in waiting {
            let mut run: Value = serde_json::from_str(&state).map_err(|error| error.to_string())?;
            let affected = run["workflow"]["stages"]
                .as_array()
                .is_some_and(|stages| stages.iter().any(|stage| stage["agent_id"] == duplicate_id));
            let has_snapshot = run["agents"]
                .as_object()
                .is_some_and(|agents| agents.contains_key(duplicate_id));
            if affected && !has_snapshot {
                if !run["agents"].is_object() {
                    run["agents"] = json!({});
                }
                run["agents"]
                    .as_object_mut()
                    .ok_or("legacy workflow run has malformed agent snapshots")?
                    .insert(duplicate_id.to_owned(), before.clone());
                tx.execute(
                    "UPDATE workflow_runs SET state=?2 WHERE id=?1 AND (?3 IS NULL OR workforce_id=?3)",
                    params![id,run.to_string(),portfolio.workforce_scope()],
                )
                .map_err(|error| error.to_string())?;
            }
        }
        tx.execute(
            "INSERT OR IGNORE INTO agent_continuations(run_id,definition,workforce_id,owner_key_id)
             SELECT DISTINCT t.run_id,?2,r.workforce_id,r.owner_key_id FROM trace_tools t
             JOIN trace_runs r ON r.id=t.run_id WHERE t.status='waiting' AND t.id LIKE ?1
               AND (?3 IS NULL OR r.workforce_id=?3)",
            params![
                format!("ccm-agent:{}:%", duplicate_id.replace('-', "")),
                before.to_string(),
                portfolio.workforce_scope()
            ],
        )
        .map_err(|error| error.to_string())?;

        let mut statement = tx
            .prepare("SELECT id,definition FROM workflows WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1)")
            .map_err(|error| error.to_string())?;
        let workflows = statement
            .query_map([portfolio.workforce_scope()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        drop(statement);
        for (id, raw) in workflows {
            let mut workflow: Workflow =
                serde_json::from_str(&raw).map_err(|error| error.to_string())?;
            let mut changed = false;
            for stage in &mut workflow.stages {
                if stage.agent_id == duplicate_id {
                    stage.agent_id = survivor_id.to_owned();
                    changed = true;
                }
            }
            if changed {
                workflow.validate()?;
                let after = serde_json::to_value(&workflow).map_err(|error| error.to_string())?;
                let affected = tx.execute(
                    "UPDATE workflows SET definition=?2 WHERE id=?1 AND definition=?3 AND retired=0
                     AND (?4 IS NULL OR workforce_id=?4)",
                    params![id,serde_json::to_string(&workflow).map_err(|error|error.to_string())?,raw,portfolio.workforce_scope()],
                ).map_err(|error|error.to_string())?;
                if affected != 1 {
                    return Err("workflow changed during consolidation application".into());
                }
                audit(
                    &tx,
                    request,
                    "workflow",
                    &id,
                    Some(&serde_json::from_str::<Value>(&raw).map_err(|error| error.to_string())?),
                    Some(&after),
                    "consolidate_references",
                    &safe_evidence,
                    portfolio.workforce_scope(),
                    portfolio.key_scope(),
                )?;
            }
        }
    }

    let affected = if kind == "agent" {
        tx.execute(
            "UPDATE agents SET retired=1 WHERE id=?1 AND name=?2 AND purpose=?3 AND system_prompt=?4
             AND model=?5 AND provider=?6 AND retired=0 AND (?7 IS NULL OR workforce_id=?7)",
            params![duplicate_id,before["name"].as_str(),before["purpose"].as_str(),before["system_prompt"].as_str(),before["model"].as_str(),before["provider"].as_str().unwrap_or(""),portfolio.workforce_scope()],
        )
    } else {
        let before_workflow: Workflow = serde_json::from_value(before.clone()).map_err(|error| error.to_string())?;
        tx.execute(
            "UPDATE workflows SET retired=1 WHERE id=?1 AND definition=?2 AND retired=0
             AND (?3 IS NULL OR workforce_id=?3)",
            params![duplicate_id,serde_json::to_string(&before_workflow).map_err(|error|error.to_string())?,portfolio.workforce_scope()],
        )
    }
    .map_err(|error| error.to_string())?;
    if affected != 1 {
        return Err("consolidation target changed during application".into());
    }
    snapshot_conn(
        &tx,
        survivor_id,
        kind,
        &current_survivor,
        portfolio.workforce_scope(),
    )
    .map_err(|error| error.to_string())?;
    snapshot_conn(&tx, duplicate_id, kind, before, portfolio.workforce_scope())
        .map_err(|error| error.to_string())?;
    tx.execute(
        "INSERT INTO consolidation_aliases(kind,duplicate_id,survivor_id,created_ms,workforce_id)
         VALUES(?1,?2,?3,?4,?5)",
        params![
            kind,
            duplicate_id,
            survivor_id,
            now_ms(),
            portfolio.workforce_scope()
        ],
    )
    .map_err(|error| error.to_string())?;
    tx.execute(
        "INSERT INTO evaluations(request_id,entity_id,entity_kind,verdict,before_definition,after_definition,
         applied,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,NULL,1,?6,?7)",
        params![request,duplicate_id,kind,json!({"action":"consolidate","survivor_id":survivor_id,"reason":"explicitly applied private-evidence suggestion; details withheld"}).to_string(),before.to_string(),portfolio.workforce_scope(),portfolio.key_scope()],
    )
    .map_err(|error| error.to_string())?;
    audit(
        &tx,
        request,
        kind,
        duplicate_id,
        Some(before),
        None,
        "consolidate",
        &safe_evidence,
        portfolio.workforce_scope(),
        portfolio.key_scope(),
    )?;
    let changed = tx
        .execute(
            "UPDATE improvement_suggestions SET status='applied',applied_ms=?4
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND status='pending'
               AND action='consolidate'",
            params![
                suggestion_id,
                portfolio.workforce_scope().unwrap_or(""),
                source_owner,
                now_ms()
            ],
        )
        .map_err(|error| error.to_string())?;
    if changed != 1 {
        return Err("consolidation suggestion was already claimed".into());
    }
    refresh_consolidation_job_receipt(
        &tx,
        &source_job_id,
        workforce_id.unwrap_or(""),
        source_owner,
        suggestion_id,
        "applied",
        is_auto_private,
    )?;
    tx.commit().map_err(|error| error.to_string())?;
    drop(conn);
    Ok(
        json!({"id":suggestion_id,"status":"applied","entity_kind":kind,"entity_id":duplicate_id,
        "action":"consolidate","survivor_id":survivor_id,"applied":true}),
    )
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) fn apply_consolidation(
    portfolio: &Portfolio,
    request: &str,
    job: &str,
    version: i64,
    registry: &Value,
    review: &Value,
    fulfilled: &Value,
    generation: u64,
) -> Result<Option<Value>, String> {
    let duplicates = review["duplicates"]
        .as_array()
        .ok_or("missing duplicates array")?;
    let mut retired = std::collections::HashSet::new();
    for item in duplicates {
        let kind = text(item, "kind")?;
        let duplicate = text(item, "duplicate_id")?;
        let survivor = text(item, "survivor_id")?;
        text(item, "reason")?;
        text(item, "evidence")?;
        find(registry, kind, duplicate)?;
        find(registry, kind, survivor)?;
        if duplicate == survivor || !retired.insert(duplicate) {
            return Err("duplicate retirement target or self-consolidation".into());
        }
    }
    if duplicates
        .iter()
        .any(|v| retired.contains(v["survivor_id"].as_str().unwrap_or("")))
    {
        return Err("consolidation chains/cycles require separate current-snapshot reviews".into());
    }
    let mut conn = portfolio.conn.lock().unwrap();
    if portfolio.generation() != generation {
        return Err("reset during consolidation".into());
    }
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    check_job(&tx, job, portfolio.workforce_scope(), portfolio.key_scope())?;
    if revision(&tx)? != version {
        return Ok(None);
    }
    // Preserve operational snapshots for continuations created before snapshot support.
    let waiting = {
        let mut stmt = tx
            .prepare("SELECT id,state FROM workflow_runs WHERE (?1 IS NULL OR workforce_id=?1)")
            .map_err(|e| e.to_string())?;
        stmt.query_map([portfolio.workforce_scope()], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?
    };
    for (id, text) in waiting {
        let mut run: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        let affected_agents = run["workflow"]["stages"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|stage| stage["agent_id"].as_str())
            .filter(|agent_id| retired.contains(agent_id))
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if !affected_agents.is_empty() {
            let mut snapshots = run["agents"].as_object().cloned().unwrap_or_default();
            for duplicate in affected_agents {
                if !snapshots.contains_key(&duplicate)
                    && let Some(definition) = definitions(registry, "agent")?
                        .iter()
                        .find(|agent| agent["id"] == duplicate)
                {
                    snapshots.insert(duplicate, definition.clone());
                }
            }
            run["agents"] = Value::Object(snapshots);
            tx.execute(
                "UPDATE workflow_runs SET state=?2 WHERE id=?1 AND (?3 IS NULL OR workforce_id=?3)",
                params![id, run.to_string(), portfolio.workforce_scope()],
            )
            .map_err(|e| e.to_string())?;
        }
    }
    for workflow in definitions(registry, "workflow")? {
        if retired.contains(workflow["id"].as_str().unwrap_or("")) {
            continue;
        }
        let mut updated: Workflow =
            serde_json::from_value(workflow.clone()).map_err(|e| e.to_string())?;
        let mut changed = false;
        for stage in &mut updated.stages {
            if let Some(item) = duplicates
                .iter()
                .find(|v| v["kind"] == "agent" && v["duplicate_id"] == stage.agent_id)
            {
                stage.agent_id = text(item, "survivor_id")?.into();
                changed = true;
            }
        }
        if changed {
            updated.validate()?;
            tx.execute(
                "UPDATE workflows SET definition=?2 WHERE id=?1 AND (?3 IS NULL OR workforce_id=?3)",
                params![
                    updated.id,
                    serde_json::to_string(&updated).map_err(|e| e.to_string())?,
                    portfolio.workforce_scope()
                ],
            )
            .map_err(|e| e.to_string())?;
            audit(
                &tx,
                request,
                "workflow",
                &updated.id,
                Some(workflow),
                Some(&json!(updated)),
                "consolidate_references",
                review,
                portfolio.workforce_scope(),
                portfolio.key_scope(),
            )?;
        }
    }
    for item in duplicates {
        let kind = text(item, "kind")?;
        let duplicate = text(item, "duplicate_id")?;
        let survivor = text(item, "survivor_id")?;
        let before = find(registry, kind, duplicate)?;
        let canonical = find(registry, kind, survivor)?;
        snapshot_conn(&tx, survivor, kind, canonical, portfolio.workforce_scope())
            .map_err(|e| e.to_string())?;
        if kind == "agent" {
            // Legacy standalone tool owners also keep their original executor.
            tx.execute("INSERT OR IGNORE INTO agent_continuations(run_id,definition,workforce_id,owner_key_id) SELECT DISTINCT t.run_id,?2,r.workforce_id,r.owner_key_id FROM trace_tools t JOIN trace_runs r ON r.id=t.run_id WHERE t.status='waiting' AND t.id LIKE ?1 AND (?3 IS NULL OR r.workforce_id=?3)",params![format!("ccm-agent:{}:%",duplicate.replace('-',"")),before.to_string(),portfolio.workforce_scope()]).map_err(|e|e.to_string())?;
            tx.execute(
                "UPDATE agents SET retired=1 WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2)",
                params![duplicate, portfolio.workforce_scope()],
            )
            .map_err(|e| e.to_string())?;
        } else {
            tx.execute(
                "UPDATE workflows SET retired=1 WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2)",
                params![duplicate, portfolio.workforce_scope()],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.execute(
            "INSERT INTO consolidation_aliases(kind,duplicate_id,survivor_id,created_ms,workforce_id) VALUES(?1,?2,?3,?4,?5)",
            params![kind, duplicate, survivor, now_ms(),portfolio.workforce_scope()],
        )
        .map_err(|e| e.to_string())?;
        tx.execute("INSERT INTO evaluations(request_id,entity_id,entity_kind,verdict,before_definition,after_definition,applied,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,NULL,1,?6,?7)",params![request,duplicate,kind,json!({"action":"consolidate","survivor_id":survivor,"survivor_definition":canonical,"reason":item["reason"],"evidence":item["evidence"]}).to_string(),before.to_string(),portfolio.workforce_scope(),portfolio.key_scope()]).map_err(|e|e.to_string())?;
        audit(
            &tx,
            request,
            kind,
            duplicate,
            Some(before),
            None,
            "consolidate",
            item,
            portfolio.workforce_scope(),
            portfolio.key_scope(),
        )?;
    }
    let outcome = json!({"fulfilled":fulfilled,"consolidation":review,"applied":true});
    tx.execute(
        "UPDATE improvement_jobs SET status='completed',outcome=?2,updated_ms=?3 WHERE id=?1 AND (?4 IS NULL OR workforce_id=?4) AND (?5 IS NULL OR owner_key_id=?5)",
        params![job, outcome.to_string(), now_ms(),portfolio.workforce_scope(),portfolio.key_scope()],
    )
    .map_err(|e| e.to_string())?;
    tx.commit().map_err(|e| e.to_string())?;
    drop(conn);
    let _ = portfolio.trace_bus.send(());
    Ok(Some(outcome))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_suggestion_apply_response_is_an_allowlisted_handle_projection() {
        let suggestion = CreationSuggestion {
            id: "private-suggestion-id".into(),
            job_id: "private-job-id".into(),
            workforce_id: "workforce-id".into(),
            owner_key_id: "source-key-id".into(),
            kind: "agent".into(),
            entity_id: String::new(),
            proposal: "private proposal".into(),
            before_definition: "null".into(),
            status: "applied".into(),
            declassified_ms: Some(42),
            idempotency_key: "private-idempotency-key".into(),
            plan_index: 0,
            writer_key_id: "peer-key-id".into(),
        };
        let result = creation_suggestion_apply_result(
            "opaque-share-handle",
            &suggestion,
            "new-agent",
            Some("completed"),
        );
        assert_eq!(
            result,
            json!({
                "id":"opaque-share-handle",
                "status":"applied",
                "action":"create",
                "kind":"agent",
                "entity_id":"new-agent",
                "applied":true,
                "receipt":true
            })
        );
        assert!(result.get("suggestion_id").is_none());
        assert!(result.get("job_status").is_none());
        assert!(!result.to_string().contains("private-job-id"));
        assert!(!result.to_string().contains("source-key-id"));

        let mut source_view = suggestion;
        source_view.writer_key_id = source_view.owner_key_id.clone();
        let source_result = creation_suggestion_apply_result(
            "private-suggestion-id",
            &source_view,
            "new-agent",
            Some("completed"),
        );
        assert_eq!(source_result["suggestion_id"], "private-suggestion-id");
        assert_eq!(source_result["job_status"], "completed");
    }

    #[test]
    fn auto_private_policy_plans_are_exact_and_generation_uses_the_persisted_bytes() {
        let requirements = "Reusable role\nwith an exact trailing space ";
        let normalized = normalize_auto_private_creation_order(&json!({
            "kind":"agent",
            "requirements":requirements,
            "dependency_for":2
        }))
        .unwrap();
        assert_eq!(
            normalized,
            json!({
                "action":"create",
                "kind":"agent",
                "requirements":requirements
            })
        );
        validate_auto_private_creation_plan(std::slice::from_ref(&normalized)).unwrap();

        let conn = Connection::open_in_memory().unwrap();
        conn.execute(
            "CREATE TABLE workforce_plans(
                job_id TEXT,plan TEXT,workforce_id TEXT,owner_key_id TEXT
             )",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO workforce_plans(job_id,plan,workforce_id,owner_key_id)
             VALUES('job',?1,'workforce','source')",
            [json!([normalized]).to_string()],
        )
        .unwrap();
        validate_creation_plan_item(
            &conn,
            "job",
            "workforce",
            "source",
            0,
            "agent",
            requirements,
        )
        .unwrap();
        assert!(
            validate_creation_plan_item(
                &conn,
                "job",
                "workforce",
                "source",
                0,
                "agent",
                "Reusable role\nwith an exact trailing space"
            )
            .is_err()
        );

        conn.execute(
            "UPDATE workforce_plans SET plan=?1 WHERE job_id='job'",
            [json!([{
                "action":"create",
                "kind":"agent",
                "requirements":requirements,
                "extra":"not allowed"
            }])
            .to_string()],
        )
        .unwrap();
        assert!(read_creation_plan(&conn, "job", "workforce", "source").is_err());
    }

    #[test]
    fn create_receipt_keys_are_stable_and_suggestion_proposals_are_exact() {
        let key = creation_idempotency_key("job-123", "workflow", 4);
        assert_eq!(key, "job-123:create:workflow:4");
        assert_eq!(creation_plan_index("job-123", "workflow", &key).unwrap(), 4);
        assert!(creation_plan_index("job-123", "workflow", "job-123:create:workflow:04").is_err());

        let proposal = json!({
            "action":"create",
            "kind":"workflow",
            "requirements":"A reusable workflow contract"
        });
        assert_eq!(
            validate_creation_suggestion(&proposal, "workflow").unwrap(),
            "A reusable workflow contract"
        );
        assert!(
            validate_creation_suggestion(
                &json!({
                    "action":"create",
                    "kind":"workflow",
                    "requirements":"A reusable workflow contract",
                    "reason":"not part of the proposal"
                }),
                "workflow"
            )
            .is_err()
        );
    }

    #[test]
    fn bounded_consolidation_coverage_matches_final_registry_and_nested_evidence_arrays() {
        let agents = (0..8)
            .map(|index| {
                json!({
                    "id":format!("agent-{index}"),
                    "name":"writer",
                    "purpose":"write",
                    "system_prompt":"agent definition ".repeat(500),
                    "review_complete":true
                })
            })
            .collect::<Vec<_>>();
        let workflows = (0..8)
            .map(|index| {
                json!({
                    "id":format!("workflow-{index}"),
                    "name":"writer workflow",
                    "purpose":"write accurately",
                    "definition":"workflow definition ".repeat(300),
                    "review_complete":true
                })
            })
            .collect::<Vec<_>>();
        let prompts = (0..32)
            .map(|turn| json!({"turn":turn + 1,"text":"prompt ".repeat(80)}))
            .collect::<Vec<_>>();
        let runs = (0..32)
            .map(|index| json!(format!("run-{index}")))
            .collect::<Vec<_>>();
        let spans = (0..32)
            .map(|_| json!({"kind":"agent_call","output":"span ".repeat(80)}))
            .collect::<Vec<_>>();
        let events = (0..32)
            .map(|_| json!({"kind":"trace_event","data":"event ".repeat(80)}))
            .collect::<Vec<_>>();
        let checkpoint_evidence = json!({
            "user_prompts":prompts,
            "conversation_execution":{"runs":runs,"spans":spans,"events":events},
            "coverage":{
                "complete":true,
                "prompts_available":32,"prompts_included":32,
                "runs_available":32,"runs_included":32,
                "spans_available_in_sampled_runs":32,"spans_included":32,
                "events_available_in_sampled_runs":32,"events_included":32
            }
        });
        let input = json!({
            "registry":{
                "agents":agents,
                "workflows":workflows,
                "coverage":{
                    "complete":true,
                    "agents_available":12,"agents_included":8,
                    "workflows_available":11,"workflows_included":8
                }
            },
            "checkpoint_evidence":checkpoint_evidence,
            "coverage":{
                "complete":true,
                "agents_available":12,"agents_included":8,
                "workflows_available":11,"workflows_included":8
            }
        });

        let bounded = bounded_consolidation_input(input);
        let parsed: Value = serde_json::from_str(&bounded).unwrap();
        let registry = &parsed["registry"];
        let agent_count = registry["agents"].as_array().unwrap().len() as u64;
        let workflow_count = registry["workflows"].as_array().unwrap().len() as u64;
        for coverage in [&parsed["coverage"], &registry["coverage"]] {
            assert_eq!(coverage["agents_included"].as_u64(), Some(agent_count));
            assert_eq!(
                coverage["workflows_included"].as_u64(),
                Some(workflow_count)
            );
            assert_eq!(
                coverage["agents_omitted_from_context"].as_u64(),
                Some(12 - agent_count)
            );
            assert_eq!(
                coverage["workflows_omitted_from_context"].as_u64(),
                Some(11 - workflow_count)
            );
        }
        let evidence = &parsed["checkpoint_evidence"];
        assert_eq!(
            evidence["coverage"]["prompts_included"].as_u64(),
            evidence["user_prompts"]
                .as_array()
                .map(|items| items.len() as u64)
        );
        for (array, included, available) in [
            ("runs", "runs_included", "runs_available"),
            ("spans", "spans_included", "spans_available_in_sampled_runs"),
            (
                "events",
                "events_included",
                "events_available_in_sampled_runs",
            ),
        ] {
            let count = evidence["conversation_execution"][array]
                .as_array()
                .unwrap()
                .len() as u64;
            assert_eq!(evidence["coverage"][included].as_u64(), Some(count));
            assert_eq!(evidence["coverage"][available].as_u64(), Some(32));
        }
        assert_eq!(parsed["coverage"]["complete"], false);
        assert!(bounded.len() <= MAX_CONSOLIDATION_INPUT_BYTES);
    }
}
