//! Embedded CCM tools, WASM plugin adapter, permission broker and configured MCP clients.
//! This module is nested under runtime so the existing binary module list stays stable.

mod embedded;
mod mcp;
mod plugins;
mod sandbox;

use crate::config::{ToolPermission, ToolingConfig};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};

#[derive(Clone, PartialEq)]
pub(crate) enum ToolTarget {
    Builtin {
        id: &'static str,
        schema: Value,
    },
    Plugin(plugins::PluginManifest),
    Mcp {
        server_id: String,
        tool_name: String,
        schema: Value,
    },
}

#[derive(Default)]
pub(crate) struct Catalog {
    pub definitions: Vec<Value>,
    pub targets: HashMap<String, ToolTarget>,
    pub names: HashSet<String>,
}

pub(crate) enum Execution {
    Completed(Value),
    Capability { tool: String, arguments: Value },
}

pub(crate) fn catalog(
    config: &ToolingConfig,
    workforce_id: &str,
    agent_id: &str,
    stage_id: Option<&str>,
) -> Result<Catalog, String> {
    config.validate()?;
    let mut catalog = Catalog::default();
    for (name, id, description, schema) in builtin_definitions() {
        let permission = permission_for(config, workforce_id, agent_id, stage_id, id);
        let configured =
            id != "shell.run" || config.shell_enabled || config.shell_sandbox_executable.is_some();
        catalog.insert(
            name,
            description,
            schema.clone(),
            ToolTarget::Builtin { id, schema },
            permission != ToolPermission::Deny && configured,
            config,
        )?;
    }
    for manifest in plugins::load_manifests_for(config, |name| {
        permission_for(
            config,
            workforce_id,
            agent_id,
            stage_id,
            &format!("plugin.{name}"),
        ) != ToolPermission::Deny
    })? {
        let function_name = plugins::function_name(&manifest.name);
        let schema = manifest.input_schema.clone();
        catalog.insert(
            function_name,
            manifest.description.clone(),
            schema,
            ToolTarget::Plugin(manifest),
            true,
            config,
        )?;
    }
    for (server_id, server) in &config.mcp_servers {
        for (tool_name, tool) in &server.tools {
            let function_name = mcp::function_name(server_id, tool_name);
            let schema = tool.input_schema.clone();
            let description = format!("MCP {}.{}: {}", server_id, tool_name, tool.description);
            let permission = permission_for(
                config,
                workforce_id,
                agent_id,
                stage_id,
                &format!("mcp.{server_id}.{tool_name}"),
            );
            catalog.insert(
                function_name,
                description,
                schema.clone(),
                ToolTarget::Mcp {
                    server_id: server_id.clone(),
                    tool_name: tool_name.clone(),
                    schema,
                },
                permission != ToolPermission::Deny,
                config,
            )?;
        }
    }
    if catalog.definitions.len() > config.max_tools {
        return Err("configured CCM and MCP tools exceed tools.max_tools".into());
    }
    let total = catalog
        .definitions
        .iter()
        .map(|definition| definition.to_string().len())
        .sum::<usize>();
    if total > config.max_total_schema_bytes {
        return Err("configured tool declarations exceed tools.max_total_schema_bytes".into());
    }
    Ok(catalog)
}

/// Provider-facing names that are eligible under the current workforce, agent,
/// and optional workflow-stage policy. `Deny` targets remain in `Catalog::targets`
/// for runtime rejection, but never appear here.
pub(crate) fn eligible_tool_names(
    config: &ToolingConfig,
    workforce_id: &str,
    agent_id: &str,
    stage_id: Option<&str>,
) -> Result<Vec<String>, String> {
    let catalog = catalog(config, workforce_id, agent_id, stage_id)?;
    let mut names = catalog
        .definitions
        .iter()
        .filter_map(|definition| {
            definition
                .pointer("/function/name")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    names.sort();
    Ok(names)
}

/// An optional Foreman selection can only narrow the invocation catalog.
pub(crate) fn validate_tool_subset(
    subset: Option<&[String]>,
    eligible: &[String],
) -> Result<(), String> {
    let Some(subset) = subset else {
        return Ok(());
    };
    let eligible = eligible.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut selected = HashSet::with_capacity(subset.len());
    for name in subset {
        if !selected.insert(name.as_str()) {
            return Err(format!("duplicate selected CCM tool: {name}"));
        }
        if !eligible.contains(name.as_str()) {
            return Err(format!(
                "selected CCM tool {name} is not eligible for this invocation"
            ));
        }
    }
    Ok(())
}

impl Catalog {
    fn insert(
        &mut self,
        name: String,
        description: String,
        schema: Value,
        target: ToolTarget,
        expose: bool,
        config: &ToolingConfig,
    ) -> Result<(), String> {
        if !valid_function_name(&name) || !self.names.insert(name.clone()) {
            return Err(format!("invalid or duplicate embedded tool name: {name}"));
        }
        validate_schema(&schema)?;
        self.targets.insert(name.clone(), target);
        if expose {
            if description.trim().is_empty() || description.len() > 2048 {
                return Err(format!("invalid embedded tool description: {name}"));
            }
            let schema_bytes = serde_json::to_vec(&schema).map_err(|error| error.to_string())?;
            if schema_bytes.len() > config.max_schema_bytes {
                return Err(format!("tool schema {name} exceeds tools.max_schema_bytes"));
            }
            self.definitions.push(json!({
                "type":"function",
                "function":{"name":name,"description":description,"parameters":schema}
            }));
        }
        Ok(())
    }
}

fn valid_function_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

pub(crate) fn builtin_definitions() -> Vec<(String, &'static str, String, Value)> {
    vec![
        (
            "ccm_file_read".into(),
            "file.read",
            "Read a bounded UTF-8 file beneath CCM's configured workspace root.".into(),
            object_schema(json!({
                "path":{"type":"string","maxLength":4096}
            }), &["path"]),
        ),
        (
            "ccm_file_write".into(),
            "file.write",
            "Write a bounded UTF-8 file beneath CCM's configured workspace root.".into(),
            object_schema(
                json!({
                    "path":{"type":"string","maxLength":4096},
                    "content":{"type":"string","maxLength":65536}
                }),
                &["path", "content"],
            ),
        ),
        (
            "ccm_shell_run".into(),
            "shell.run",
            "Run a command string in the configured workspace; sandboxing follows tools.sandbox_enabled and the platform default.".into(),
            object_schema(json!({
                "command":{"type":"string","maxLength":4096}
            }), &["command"]),
        ),
        (
            "ccm_http_request".into(),
            "http.request",
            "Make a bounded HTTP request to a host in the administrator's exact host allowlist.".into(),
            object_schema(
                json!({
                    "method":{"type":"string","enum":["GET","POST","PUT","DELETE"],"maxLength":6},
                    "url":{"type":"string","maxLength":4096},
                    "headers":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string","maxLength":128},"value":{"type":"string","maxLength":2048}},"required":["name","value"],"additionalProperties":false},"maxItems":16},
                    "body":{"type":"string","maxLength":65536}
                }),
                &["method", "url"],
            ),
        ),
    ]
}

fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type":"object",
        "properties":properties,
        "required":required,
        "additionalProperties":false
    })
}

/// Validate the bounded JSON-Schema subset used by tools and plugins. Remote refs,
/// arbitrary keywords, and schemas with unbounded depth are intentionally rejected.
pub(crate) fn validate_schema(schema: &Value) -> Result<(), String> {
    if schema.get("type").and_then(Value::as_str) != Some("object") {
        return Err("tool argument schema root must be an object".into());
    }
    validate_schema_at(schema, 0)
}

fn validate_schema_at(schema: &Value, depth: usize) -> Result<(), String> {
    if depth > 8 {
        return Err("tool schema nesting exceeds eight levels".into());
    }
    let object = schema
        .as_object()
        .ok_or("tool schema must be a JSON object")?;
    if object.keys().any(|key| {
        ![
            "type",
            "properties",
            "required",
            "additionalProperties",
            "description",
            "enum",
            "items",
            "maxLength",
            "maxItems",
            "maxProperties",
        ]
        .contains(&key.as_str())
    }) {
        return Err("tool schema contains unsupported or unbounded keywords".into());
    }
    let schema_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or("tool schema type is required")?;
    if !["object", "array", "string", "number", "integer", "boolean"].contains(&schema_type) {
        return Err("tool schema has unsupported type".into());
    }
    if object
        .get("description")
        .is_some_and(|v| v.as_str().is_none_or(|s| s.len() > 512))
    {
        return Err("tool schema description is invalid or too long".into());
    }
    if let Some(enumeration) = object.get("enum") {
        let values = enumeration
            .as_array()
            .ok_or("tool schema enum must be an array")?;
        if values.is_empty()
            || values.len() > 64
            || values.iter().any(|v| v.to_string().len() > 512)
        {
            return Err("tool schema enum exceeds bounds".into());
        }
    }
    if schema_type == "object" {
        if object.get("additionalProperties") != Some(&Value::Bool(false)) {
            return Err("tool object schemas must set additionalProperties=false".into());
        }
        let properties = object
            .get("properties")
            .and_then(Value::as_object)
            .ok_or("tool object schema requires properties")?;
        if properties.len() > 64 {
            return Err("tool schema has too many properties".into());
        }
        if let Some(required) = object.get("required") {
            let required = required
                .as_array()
                .ok_or("tool schema required must be an array")?;
            if required.len() > properties.len()
                || required.iter().any(|name| {
                    name.as_str()
                        .is_none_or(|name| !properties.contains_key(name))
                })
            {
                return Err("tool schema required properties are invalid".into());
            }
        }
        for property in properties.values() {
            validate_schema_at(property, depth + 1)?;
        }
    }
    if schema_type == "array" {
        let items = object
            .get("items")
            .ok_or("tool array schema requires items")?;
        if object
            .get("maxItems")
            .and_then(Value::as_u64)
            .is_none_or(|max| max == 0 || max > 1024)
        {
            return Err("tool array schemas require bounded maxItems".into());
        }
        validate_schema_at(items, depth + 1)?;
    }
    if schema_type == "string"
        && object
            .get("maxLength")
            .and_then(Value::as_u64)
            .is_none_or(|max| max == 0 || max > 65_536)
    {
        return Err("tool string schemas require bounded maxLength".into());
    }
    Ok(())
}

pub(crate) fn validate_arguments(schema: &Value, value: &Value) -> Result<(), String> {
    validate_schema(schema)?;
    validate_value(schema, value, 0)
}

fn validate_value(schema: &Value, value: &Value, depth: usize) -> Result<(), String> {
    if depth > 8 {
        return Err("tool arguments nesting exceeds eight levels".into());
    }
    let schema_type = schema["type"].as_str().ok_or("invalid tool schema")?;
    let matches = match schema_type {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "number" => value.is_number(),
        "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        _ => false,
    };
    if !matches {
        return Err(format!("tool argument must have type {schema_type}"));
    }
    if schema_type == "object" {
        let object = value
            .as_object()
            .ok_or("tool arguments must be an object")?;
        let properties = schema["properties"]
            .as_object()
            .ok_or("invalid properties")?;
        if object.keys().any(|key| !properties.contains_key(key)) {
            return Err("tool arguments contain undeclared properties".into());
        }
        if let Some(required) = schema["required"].as_array() {
            if required
                .iter()
                .filter_map(Value::as_str)
                .any(|key| !object.contains_key(key))
            {
                return Err("tool arguments are missing required properties".into());
            }
        }
        for (key, value) in object {
            validate_value(&properties[key], value, depth + 1)?;
        }
    }
    if schema_type == "array" {
        let values = value.as_array().ok_or("tool argument must be an array")?;
        if values.len() > schema["maxItems"].as_u64().unwrap_or(0) as usize {
            return Err("tool array argument exceeds maxItems".into());
        }
        for value in values {
            validate_value(&schema["items"], value, depth + 1)?;
        }
    }
    if schema_type == "string" {
        let value = value.as_str().ok_or("tool argument must be a string")?;
        if value.len() > schema["maxLength"].as_u64().unwrap_or(0) as usize {
            return Err("tool string argument exceeds maxLength".into());
        }
    }
    if let Some(enumeration) = schema.get("enum")
        && !enumeration
            .as_array()
            .is_some_and(|values| values.contains(value))
    {
        return Err("tool argument is outside its allowed enum".into());
    }
    Ok(())
}

pub(crate) fn permission_for(
    config: &ToolingConfig,
    workforce_id: &str,
    agent_id: &str,
    stage_id: Option<&str>,
    tool: &str,
) -> ToolPermission {
    let Some(policy) = config.workforces.get(workforce_id) else {
        return ToolPermission::Deny;
    };
    let grants = &policy.grants;
    if let Some(stage) = stage_id
        && let Some(grant) = grants.iter().find(|grant| {
            grant.tool == tool
                && grant.agent_id.as_deref() == Some(agent_id)
                && grant.stage_id.as_deref() == Some(stage)
        })
    {
        return grant.decision;
    }
    if let Some(grant) = grants.iter().find(|grant| {
        grant.tool == tool
            && grant.agent_id.as_deref() == Some(agent_id)
            && grant.stage_id.is_none()
    }) {
        return grant.decision;
    }
    if let Some(grant) = grants
        .iter()
        .find(|grant| grant.tool == tool && grant.agent_id.is_none() && grant.stage_id.is_none())
    {
        return grant.decision;
    }
    policy.default
}

pub(crate) fn permission_denied(tool: &str) -> Value {
    json!({"error":"blocked by CCM permission policy","tool":tool})
}

pub(crate) async fn execute(
    config: &ToolingConfig,
    target: &ToolTarget,
    arguments: &Value,
) -> Result<Execution, String> {
    match target {
        ToolTarget::Builtin { id, schema } => {
            validate_arguments(schema, arguments)?;
            embedded::execute(config, id, arguments)
                .await
                .map(Execution::Completed)
        }
        ToolTarget::Plugin(manifest) => {
            validate_arguments(&manifest.input_schema, arguments)?;
            let output = plugins::invoke(config, manifest, arguments.clone()).await?;
            if let Some(action) = output.get("tool").and_then(Value::as_str) {
                if !manifest
                    .capabilities
                    .iter()
                    .any(|capability| capability == action)
                {
                    return Err("plugin requested a capability not declared by its manifest".into());
                }
                let action_args = output
                    .get("arguments")
                    .ok_or("plugin action is missing arguments")?;
                let Some((_, id, _, schema)) = builtin_definitions()
                    .into_iter()
                    .find(|(_, id, _, _)| *id == action)
                else {
                    return Err("plugin requested an unsupported CCM capability".into());
                };
                validate_arguments(&schema, action_args)?;
                Ok(Execution::Capability {
                    tool: id.into(),
                    arguments: action_args.clone(),
                })
            } else {
                Ok(Execution::Completed(output))
            }
        }
        ToolTarget::Mcp {
            server_id,
            tool_name,
            schema,
        } => {
            validate_arguments(schema, arguments)?;
            let server = config
                .mcp_servers
                .get(server_id)
                .ok_or("MCP server is not configured")?;
            if !server.profile.reviewed {
                return Err("MCP provider profile is not administrator-reviewed".into());
            }
            mcp::call(config, server, tool_name, arguments)
                .await
                .map(Execution::Completed)
        }
    }
}

pub(crate) fn builtin_target(tool: &str) -> Option<ToolTarget> {
    builtin_definitions()
        .into_iter()
        .find(|(_, id, _, _)| *id == tool)
        .map(|(_, id, _, schema)| ToolTarget::Builtin { id, schema })
}

pub(crate) fn tool_id(target: &ToolTarget) -> String {
    match target {
        ToolTarget::Builtin { id, .. } => (*id).into(),
        ToolTarget::Plugin(manifest) => format!("plugin.{}", manifest.name),
        ToolTarget::Mcp {
            server_id,
            tool_name,
            ..
        } => format!("mcp.{server_id}.{tool_name}"),
    }
}

pub(crate) fn target_schema(target: &ToolTarget) -> &Value {
    match target {
        ToolTarget::Builtin { schema, .. } => schema,
        ToolTarget::Plugin(manifest) => &manifest.input_schema,
        ToolTarget::Mcp { schema, .. } => schema,
    }
}

pub(crate) fn pending_result_cap(config: &ToolingConfig, result: &Value) -> Result<Value, String> {
    let encoded = serde_json::to_vec(result).map_err(|error| error.to_string())?;
    if encoded.len() > config.max_result_bytes {
        return Err("tool result exceeds tools.max_result_bytes".into());
    }
    Ok(result.clone())
}

pub(crate) fn sandbox_helper_entrypoint() -> bool {
    sandbox::helper_entrypoint()
}

pub(crate) fn validate_sandbox_config(config: &ToolingConfig) -> Result<(), String> {
    sandbox::validate_config(config)
}

pub(crate) fn resolve_sandbox_mode(
    config: &ToolingConfig,
    mcp_override: Option<bool>,
) -> Result<sandbox::SandboxMode, String> {
    sandbox::resolve_mode(config.sandbox_enabled, mcp_override)
}

pub(crate) fn sandbox_runtime_status(config: &ToolingConfig) -> Value {
    sandbox::runtime_status(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ToolGrant, WorkforceToolPolicy};
    use std::collections::BTreeMap;

    #[test]
    fn missing_workforce_and_grants_deny_by_default_and_are_isolated() {
        let mut config = ToolingConfig::default();
        config.workforces.insert(
            "workforce-a".into(),
            WorkforceToolPolicy {
                default: ToolPermission::Allow,
                grants: vec![ToolGrant {
                    tool: "file.write".into(),
                    decision: ToolPermission::Deny,
                    ..Default::default()
                }],
            },
        );
        assert_eq!(
            permission_for(&config, "missing", "agent", None, "file.read"),
            ToolPermission::Deny
        );
        assert_eq!(
            permission_for(&config, "workforce-a", "agent", None, "file.read"),
            ToolPermission::Allow
        );
        assert_eq!(
            permission_for(&config, "workforce-a", "agent", None, "file.write"),
            ToolPermission::Deny
        );
        assert_eq!(
            permission_for(&config, "workforce-b", "agent", None, "file.read"),
            ToolPermission::Deny
        );
    }

    #[test]
    fn stage_grants_override_agent_and_workforce_defaults() {
        let config = ToolingConfig {
            workforces: BTreeMap::from([(
                "wf".into(),
                WorkforceToolPolicy {
                    default: ToolPermission::Deny,
                    grants: vec![
                        ToolGrant {
                            tool: "http.request".into(),
                            decision: ToolPermission::Allow,
                            agent_id: Some("agent".into()),
                            stage_id: None,
                        },
                        ToolGrant {
                            tool: "http.request".into(),
                            decision: ToolPermission::Ask,
                            agent_id: Some("agent".into()),
                            stage_id: Some("review".into()),
                        },
                    ],
                },
            )]),
            ..Default::default()
        };
        assert_eq!(
            permission_for(&config, "wf", "agent", Some("review"), "http.request"),
            ToolPermission::Ask
        );
        assert_eq!(
            permission_for(&config, "wf", "agent", Some("execute"), "http.request"),
            ToolPermission::Allow
        );
        assert_eq!(
            permission_for(&config, "wf", "another", Some("review"), "http.request"),
            ToolPermission::Deny
        );
    }

    #[test]
    fn shell_tool_requires_shell_opt_in_as_well_as_exact_workforce_grant() {
        let mut config = ToolingConfig {
            sandbox_enabled: Some(false),
            workforces: BTreeMap::from([(
                "wf".into(),
                WorkforceToolPolicy {
                    default: ToolPermission::Deny,
                    grants: vec![ToolGrant {
                        tool: "shell.run".into(),
                        decision: ToolPermission::Allow,
                        agent_id: Some("agent".into()),
                        stage_id: None,
                    }],
                },
            )]),
            ..Default::default()
        };

        let shell_advertised = |catalog: &Catalog| {
            catalog
                .definitions
                .iter()
                .any(|definition| definition["function"]["name"] == "ccm_shell_run")
        };
        assert!(!shell_advertised(
            &catalog(&config, "wf", "agent", None).unwrap()
        ));

        // Disabling the sandbox does not independently enable shell access;
        // the shell opt-in and the exact workforce grant still apply.
        config.shell_enabled = false;
        assert!(!shell_advertised(
            &catalog(&config, "wf", "agent", None).unwrap()
        ));

        config.shell_enabled = true;
        assert!(shell_advertised(
            &catalog(&config, "wf", "agent", None).unwrap()
        ));
        assert!(!shell_advertised(
            &catalog(&config, "wf", "other-agent", None).unwrap()
        ));
    }

    #[test]
    fn invocation_catalog_contains_only_allow_and_ask_tools_for_the_selected_stage() {
        let config = ToolingConfig {
            workforces: BTreeMap::from([(
                "wf".into(),
                WorkforceToolPolicy {
                    default: ToolPermission::Deny,
                    grants: vec![
                        ToolGrant {
                            tool: "file.read".into(),
                            decision: ToolPermission::Allow,
                            agent_id: Some("agent".into()),
                            stage_id: None,
                        },
                        ToolGrant {
                            tool: "file.write".into(),
                            decision: ToolPermission::Ask,
                            agent_id: Some("agent".into()),
                            stage_id: Some("review".into()),
                        },
                        ToolGrant {
                            tool: "shell.run".into(),
                            decision: ToolPermission::Allow,
                            agent_id: Some("other-agent".into()),
                            stage_id: None,
                        },
                    ],
                },
            )]),
            ..Default::default()
        };
        let work = catalog(&config, "wf", "agent", None).unwrap();
        let review = catalog(&config, "wf", "agent", Some("review")).unwrap();
        let names = |catalog: &Catalog| {
            catalog
                .definitions
                .iter()
                .map(|tool| {
                    tool.pointer("/function/name")
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_owned()
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(names(&work), ["ccm_file_read"]);
        assert_eq!(names(&review), ["ccm_file_read", "ccm_file_write"]);
        assert!(
            review.targets.contains_key("ccm_shell_run"),
            "the runtime keeps a deny target so guessed calls are rejected by policy"
        );
    }

    #[test]
    fn foreman_subset_can_only_narrow_the_eligible_invocation_catalog() {
        let eligible = vec!["ccm_file_read".to_owned(), "ccm_file_write".to_owned()];
        validate_tool_subset(None, &eligible).unwrap();
        validate_tool_subset(Some(&[]), &eligible).unwrap();
        validate_tool_subset(Some(&["ccm_file_write".into()]), &eligible).unwrap();
        assert!(validate_tool_subset(Some(&["ccm_shell_run".into()]), &eligible).is_err());
        assert!(
            validate_tool_subset(
                Some(&["ccm_file_read".into(), "ccm_file_read".into()]),
                &eligible
            )
            .is_err()
        );
    }

    #[test]
    fn invalid_unselected_plugin_does_not_block_an_invocation_but_selected_one_fails_closed() {
        let root =
            std::env::temp_dir().join(format!("ccm-plugin-catalog-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join("bad.json"),
            r#"{"name":"bad","description":"broken but unselected","wasm":"missing.wasm","input_schema":{"type":"unsupported","properties":{},"additionalProperties":false}}"#,
        )
        .unwrap();
        let mut config = ToolingConfig {
            plugin_dir: root.to_string_lossy().into_owned(),
            ..Default::default()
        };
        assert!(
            catalog(&config, "wf", "agent", None)
                .unwrap()
                .definitions
                .is_empty()
        );
        config.workforces.insert(
            "wf".into(),
            WorkforceToolPolicy {
                default: ToolPermission::Deny,
                grants: vec![ToolGrant {
                    tool: "plugin.bad".into(),
                    decision: ToolPermission::Allow,
                    agent_id: Some("agent".into()),
                    stage_id: None,
                }],
            },
        );
        assert!(catalog(&config, "wf", "agent", None).is_err());
        std::fs::remove_file(root.join("bad.json")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[test]
    fn exposed_invocation_catalog_obeys_tool_and_schema_budgets() {
        let config = ToolingConfig {
            max_schema_bytes: 512,
            max_total_schema_bytes: 512,
            max_tools: 4,
            workforces: BTreeMap::from([(
                "wf".into(),
                WorkforceToolPolicy {
                    default: ToolPermission::Allow,
                    grants: Vec::new(),
                },
            )]),
            ..Default::default()
        };
        assert!(catalog(&config, "wf", "agent", None).is_err());
    }

    #[test]
    fn malformed_and_unbounded_schemas_or_arguments_are_rejected() {
        assert!(validate_schema(&json!({"type":"object","properties":{}})).is_err());
        assert!(validate_schema(&json!({"$ref":"https://example.invalid/schema"})).is_err());
        let schema = object_schema(json!({"name":{"type":"string","maxLength":4}}), &["name"]);
        assert!(validate_arguments(&schema, &json!({"name":"too-long"})).is_err());
        assert!(validate_arguments(&schema, &json!({"name":"ok","extra":true})).is_err());
    }
}
