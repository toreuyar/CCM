//! Routing-only protocol. The same bounded schema drives provider constraints and
//! local validation; runtime authorization still rechecks immediately before action.
use crate::{config::PersonalityTransparency, policy, state::AppState};
use serde_json::{Value, json};

fn text() -> Value {
    json!({"type":"string","minLength":1})
}
fn choices(values: Vec<String>) -> Value {
    json!({"type":"string","enum":values})
}
fn object(properties: Value, required: Vec<&str>) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn array(items: Value, min_items: usize, max_items: usize) -> Value {
    json!({"type":"array","items":items,"minItems":min_items,"maxItems":max_items})
}
fn tool_subset(names: Vec<String>) -> Value {
    let max_items = names.len();
    // JSON Schema `enum` must be nonempty. A zero maxItems array may use an
    // unreachable placeholder when this executor has no eligible CCM tools.
    let values = if names.is_empty() {
        vec!["__no_eligible_ccm_tool__".into()]
    } else {
        names
    };
    array(choices(values), 0, max_items)
}
fn eligible_tool_names(
    config: &crate::config::ToolingConfig,
    workforce: &str,
    agent: &str,
    stage: Option<&str>,
) -> Vec<String> {
    match crate::runtime::tools::eligible_tool_names(config, workforce, agent, stage) {
        Ok(names) => names,
        Err(error) => {
            // Catalog construction is executor-scoped: a broken eligible plug-in
            // for one route must not invalidate unrelated routing alternatives.
            tracing::warn!(%error, workforce, agent, ?stage, "Foreman tool-selection catalog unavailable");
            Vec::new()
        }
    }
}
fn decision(name: &str, extra: Value, required: Vec<&str>) -> Value {
    let mut properties = json!({
        "decision":{"const":name},
        "note":{"type":"string"},
        "chat_narration_allowed":{"type":"boolean"},
        "execution_mode":{"type":"string","enum":["sync","async"]},
        "authorization_quote":{"type":"string","minLength":1}
    });
    properties
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let mut required = required;
    required.push("decision");
    object(properties, required)
}

pub fn schema(state: &AppState, context: &policy::Context) -> Result<Value, String> {
    let agents: Vec<String> = state
        .portfolio
        .list()
        .map_err(|e| e.to_string())?
        .into_iter()
        .map(|a| a.id)
        .collect();
    let workflows = state.portfolio.workflow_list()?;
    let workflow_ids = workflows
        .iter()
        .map(|workflow| workflow.id.clone())
        .collect::<Vec<_>>();
    let tool_config = crate::config::tooling_config();
    let workforce = state.portfolio.workforce_scope().unwrap_or("anonymous");
    let mut variants = vec![];
    for agent_id in &agents {
        let names = eligible_tool_names(&tool_config, workforce, agent_id, None);
        variants.push(decision(
            "use",
            json!({
                "agent_id":{"const":agent_id},
                "tool_subset":tool_subset(names)
            }),
            vec!["agent_id"],
        ));
    }
    if agents.len() >= 2 {
        let mut task_variants = Vec::with_capacity(agents.len());
        for agent_id in &agents {
            let names = eligible_tool_names(&tool_config, workforce, agent_id, None);
            task_variants.push(object(
                json!({
                    "agent_id":{"const":agent_id},
                    "instruction":text(),
                    "tool_subset":tool_subset(names)
                }),
                vec!["agent_id", "instruction"],
            ));
        }
        variants.push(decision(
            "fan_out",
            json!({"tasks":array(json!({"anyOf":task_variants}), 2, 4)}),
            vec!["tasks"],
        ));
    }
    for workflow in &workflows {
        workflow.validate()?;
        let mut stage_properties = serde_json::Map::new();
        let mut stage_ids = Vec::with_capacity(workflow.stages.len());
        for stage in &workflow.stages {
            let names =
                eligible_tool_names(&tool_config, workforce, &stage.agent_id, Some(&stage.id));
            stage_ids.push(stage.id.as_str());
            stage_properties.insert(stage.id.clone(), tool_subset(names));
        }
        variants.push(decision(
            "use_workflow",
            json!({
                "workflow_id":{"const":workflow.id},
                "stage_tool_subsets":object(
                    Value::Object(stage_properties),
                    stage_ids
                )
            }),
            vec!["workflow_id"],
        ));
    }
    let fallback_names = eligible_tool_names(&tool_config, workforce, policy::FALLBACK_ID, None);
    variants.push(decision(
        "fallback",
        json!({"tool_subset":tool_subset(fallback_names)}),
        vec![],
    ));
    if context.phase.as_deref() == Some("pre") {
        for name in ["create", "create_workflow"] {
            variants.push(decision(
                name,
                json!({"requirements":text()}),
                vec!["requirements"],
            ));
        }
    }
    let mut tools = policy::tools(context);
    tools.extend(crate::management::foreman_tool_specs(state));
    if state.config.personality_transparency == PersonalityTransparency::Flexible {
        tools.push(json!({"name":"set_personality"}));
    }
    let mut tool_names = std::collections::HashSet::new();
    for tool in tools {
        let name = tool["name"].as_str().ok_or("internal tool lacks name")?;
        if !tool_names.insert(name.to_owned()) {
            continue;
        }
        let args = match name {
            "list_agents" | "list_workflows" | "get_statistics" | "get_evaluations" => {
                object(json!({}), vec![])
            }
            "create_agent" | "create_workflow" => {
                object(json!({"requirements":text()}), vec!["requirements"])
            }
            "defer_creation" => object(
                json!({"kind":{"enum":["agent","workflow"]},"requirements":text()}),
                vec!["kind", "requirements"],
            ),
            "set_creation_timing" => object(
                json!({"timing":{"enum":["pre","post"]},"user_instruction":text()}),
                if context.policy.is_default() {
                    vec!["timing", "user_instruction"]
                } else {
                    vec!["timing"]
                },
            ),
            "set_personality" => object(
                json!({"mode":{"enum":["introvert","extrovert"]},"user_instruction":text()}),
                vec!["mode", "user_instruction"],
            ),
            "evaluate_agent" | "evaluate_workflow" => {
                let ids = if name == "evaluate_agent" {
                    &agents
                } else {
                    &workflow_ids
                };
                if ids.is_empty() {
                    continue;
                }
                object(
                    json!({"id":choices(ids.clone()),"task":text(),"result":text()}),
                    vec!["id", "task", "result"],
                )
            }
            "management.server_context" => object(json!({}), vec![]),
            "management.inspect_definition" => object(
                json!({
                    "kind":{"type":"string","enum":["agent","workflow"]},
                    "id":text(),
                    "include_retired":{"type":"boolean"}
                }),
                vec!["kind", "id"],
            ),
            "management.inspect_history" => object(
                json!({
                    "kind":{"type":"string","enum":["agent","workflow"]},
                    "id":text()
                }),
                vec!["kind", "id"],
            ),
            "management.create_definition" => object(
                json!({
                    "kind":{"type":"string","enum":["agent","workflow"]},
                    "requirements":text(),
                    "user_instruction":text()
                }),
                vec!["kind", "requirements", "user_instruction"],
            ),
            "management.evaluate_definition" => object(
                json!({
                    "kind":{"type":"string","enum":["agent","workflow"]},
                    "id":text(),
                    "task":text(),
                    "result":text(),
                    "user_instruction":text()
                }),
                vec!["kind", "id", "task", "result", "user_instruction"],
            ),
            "management.retire_definition" => object(
                json!({
                    "kind":{"type":"string","enum":["agent","workflow"]},
                    "id":text(),
                    "user_instruction":text()
                }),
                vec!["kind", "id", "user_instruction"],
            ),
            "management.get_settings" => object(
                json!({
                    "scope":{
                        "type":"string",
                        "enum":if state.auth.as_ref().is_some_and(|auth| auth.permissions.bootstrap_admin) {
                            vec!["current_key", "global"]
                        } else {
                            vec!["current_key"]
                        }
                    }
                }),
                vec!["scope"],
            ),
            "management.update_settings" => object(
                json!({
                    "scope":{
                        "type":"string",
                        "enum":if state.auth.as_ref().is_some_and(|auth| auth.permissions.bootstrap_admin) {
                            vec!["current_key", "global"]
                        } else {
                            vec!["current_key"]
                        }
                    },
                    "revision":text(),
                    "changes":{"type":"object","minProperties":1,"additionalProperties":true},
                    "user_instruction":text()
                }),
                vec!["scope", "revision", "changes", "user_instruction"],
            ),
            _ => return Err("tool missing routing schema".into()),
        };
        variants.push(decision(
            "tool",
            json!({"name":{"const":name},"arguments":args}),
            vec!["name", "arguments"],
        ));
    }
    Ok(json!({"anyOf":variants}))
}

/// Validate precisely the schema subset constructed above. No arbitrary schemas
/// or model-authored validation rules are accepted here.
fn validate_node(value: &Value, schema: &Value, path: &str) -> Result<(), String> {
    if let Some(variants) = schema.get("anyOf").and_then(Value::as_array) {
        if variants
            .iter()
            .any(|variant| validate_node(value, variant, path).is_ok())
        {
            return Ok(());
        }
        return Err(format!("{path}: value does not match an available variant"));
    }
    if let Some(expected) = schema.get("const") {
        if value != expected {
            return Err(format!("{path}: value is not permitted"));
        }
    }
    if let Some(allowed) = schema["enum"].as_array() {
        if !allowed.contains(value) {
            return Err(format!("{path}: value is not currently available"));
        }
    }
    match schema["type"].as_str() {
        Some("object") => {
            let object = value
                .as_object()
                .ok_or_else(|| format!("{path}: expected object"))?;
            let properties = schema["properties"]
                .as_object()
                .ok_or("invalid internal schema")?;
            for name in schema["required"]
                .as_array()
                .ok_or("invalid internal required fields")?
            {
                let name = name.as_str().ok_or("invalid internal field")?;
                if !object.contains_key(name) {
                    return Err(format!("{path}.{name}: required field missing"));
                }
            }
            for (name, value) in object {
                let field = properties
                    .get(name)
                    .ok_or_else(|| format!("{path}: unexpected field"))?;
                validate_node(value, field, &format!("{path}.{name}"))?;
            }
        }
        Some("array") => {
            let items = value
                .as_array()
                .ok_or_else(|| format!("{path}: expected array"))?;
            if schema["minItems"]
                .as_u64()
                .is_some_and(|minimum| items.len() < minimum as usize)
                || schema["maxItems"]
                    .as_u64()
                    .is_some_and(|maximum| items.len() > maximum as usize)
            {
                return Err(format!(
                    "{path}: array length is outside the permitted range"
                ));
            }
            let item_schema = schema.get("items").ok_or("invalid internal array schema")?;
            for (index, item) in items.iter().enumerate() {
                validate_node(item, item_schema, &format!("{path}[{index}]"))?;
            }
        }
        Some("string") => {
            let text = value
                .as_str()
                .ok_or_else(|| format!("{path}: expected string"))?;
            if schema["minLength"].as_u64().is_some_and(|n| n > 0) && text.trim().is_empty() {
                return Err(format!("{path}: nonempty string required"));
            }
        }
        Some("boolean") if !value.is_boolean() => return Err(format!("{path}: expected boolean")),
        _ => {}
    }
    Ok(())
}

fn validate_unique_tool_subset(value: &Value, path: &str) -> Result<(), String> {
    let Some(names) = value.as_array() else {
        return Err(format!("{path}: expected tool-name array"));
    };
    let mut seen = std::collections::HashSet::new();
    for name in names {
        let name = name
            .as_str()
            .ok_or_else(|| format!("{path}: tool names must be strings"))?;
        if !seen.insert(name) {
            return Err(format!("{path}: duplicate tool name {name}"));
        }
    }
    Ok(())
}

fn validate_tool_subsets(value: &Value) -> Result<(), String> {
    if let Some(subset) = value.get("tool_subset") {
        validate_unique_tool_subset(subset, "decision.tool_subset")?;
    }
    if let Some(tasks) = value.get("tasks").and_then(Value::as_array) {
        for (index, task) in tasks.iter().enumerate() {
            if let Some(subset) = task.get("tool_subset") {
                validate_unique_tool_subset(
                    subset,
                    &format!("decision.tasks[{index}].tool_subset"),
                )?;
            }
        }
    }
    if let Some(stages) = value.get("stage_tool_subsets").and_then(Value::as_object) {
        for (stage, subset) in stages {
            validate_unique_tool_subset(subset, &format!("decision.stage_tool_subsets.{stage}"))?;
        }
    }
    Ok(())
}

pub fn validate(content: &str, schema: &Value) -> Result<Value, String> {
    let value: Value = serde_json::from_str(content)
        .map_err(|_| "return exactly one JSON object, without prose or markdown")?;
    if !value.is_object() {
        return Err("routing decision must be a JSON object".into());
    }
    let variants = schema["anyOf"]
        .as_array()
        .ok_or("invalid internal decision schema")?;
    let variant = variants
        .iter()
        .find(|v| {
            v["properties"]["decision"]["const"] == value["decision"]
                && (value["decision"] != "tool"
                    || v["properties"]["name"]["const"] == value["name"])
        })
        .ok_or("decision or tool is unavailable under the current creation/personality policy")?;
    validate_node(&value, variant, "decision")?;
    if value["decision"] == "fan_out" {
        let mut ids = std::collections::HashSet::new();
        for task in value["tasks"]
            .as_array()
            .ok_or("fan-out tasks must be an array")?
        {
            let id = task["agent_id"]
                .as_str()
                .ok_or("fan-out task agent_id must be a string")?;
            if !ids.insert(id) {
                return Err("fan-out tasks must use distinct agent ids".into());
            }
        }
    }
    validate_tool_subsets(&value)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_schema_rejects_tool_from_another_agent_and_duplicate_selection() {
        let agent_a = decision(
            "use",
            json!({
                "agent_id":{"const":"agent-a"},
                "tool_subset":tool_subset(vec!["ccm_file_read".into()])
            }),
            vec!["agent_id"],
        );
        let schema = json!({"anyOf":[agent_a]});
        let other_agent_tool = json!({
            "decision":"use",
            "agent_id":"agent-a",
            "tool_subset":["ccm_shell_run"]
        });
        assert!(validate(&other_agent_tool.to_string(), &schema).is_err());

        let duplicate = json!({
            "decision":"use",
            "agent_id":"agent-a",
            "tool_subset":["ccm_file_read","ccm_file_read"]
        });
        assert!(validate(&duplicate.to_string(), &schema).is_err());
    }

    #[test]
    fn stage_and_fanout_subsets_are_scoped_to_each_executor() {
        let stage_map = object(
            json!({"write":tool_subset(vec!["ccm_file_write".into()])}),
            vec!["write"],
        );
        let workflow = decision(
            "use_workflow",
            json!({"workflow_id":{"const":"wf"},"stage_tool_subsets":stage_map}),
            vec!["workflow_id"],
        );
        let task_a = object(
            json!({
                "agent_id":{"const":"agent-a"},
                "instruction":text(),
                "tool_subset":tool_subset(vec!["ccm_file_read".into()])
            }),
            vec!["agent_id", "instruction"],
        );
        let task_b = object(
            json!({
                "agent_id":{"const":"agent-b"},
                "instruction":text(),
                "tool_subset":tool_subset(vec!["ccm_shell_run".into()])
            }),
            vec!["agent_id", "instruction"],
        );
        let fanout = decision(
            "fan_out",
            json!({"tasks":array(json!({"anyOf":[task_a,task_b]}), 2, 4)}),
            vec!["tasks"],
        );
        let schema = json!({"anyOf":[workflow,fanout]});
        assert!(validate(
            r#"{"decision":"use_workflow","workflow_id":"wf","stage_tool_subsets":{"write":["ccm_shell_run"]}}"#,
            &schema
        )
        .is_err());
        assert!(validate(
            r#"{"decision":"fan_out","tasks":[{"agent_id":"agent-a","instruction":"a","tool_subset":["ccm_shell_run"]},{"agent_id":"agent-b","instruction":"b","tool_subset":["ccm_shell_run"]}]}"#,
            &schema
        )
        .is_err());
        assert!(validate(
            r#"{"decision":"fan_out","tasks":[{"agent_id":"agent-a","instruction":"a","tool_subset":["ccm_file_read"]},{"agent_id":"agent-b","instruction":"b","tool_subset":["ccm_shell_run"]}]}"#,
            &schema
        )
        .is_ok());
    }
}
