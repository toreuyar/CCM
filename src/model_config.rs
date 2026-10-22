//! Exposed model routing, operational inheritance and credential-free run snapshots.
use crate::{
    config::{Config, PersonalityTransparency},
    state::AppState,
};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Provider {
    pub base_url: String,
    /// Credentials are resolved from the environment, never stored in snapshots.
    pub api_key_env: Option<String>,
    /// Enable only for providers verified to enforce OpenAI JSON Schema output.
    #[serde(default)]
    pub structured_output: bool,
    #[serde(default)]
    pub max_concurrent_requests: Option<usize>,
    #[serde(default)]
    pub recovery: crate::provider_gate::Recovery,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Assignment {
    pub provider: Option<String>,
    pub model: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Candidate {
    pub provider: String,
    pub model: String,
    pub context_tokens: u64,
    pub capabilities: Vec<String>,
    pub reasoning: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Timeouts {
    pub connect_timeout_ms: Option<u64>,
    pub first_response_timeout_ms: Option<u64>,
    pub idle_timeout_ms: Option<u64>,
    pub total_timeout_ms: Option<u64>,
    pub max_attempts: Option<u32>,
    pub retry_base_ms: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Overrides {
    pub provider: Option<String>,
    /// Actual inference model ID, not an exposed alias.
    pub model: Option<String>,
    pub personality_transparency: Option<PersonalityTransparency>,
    pub creation_policy: Option<crate::policy::CreationPolicy>,
    pub creation_prompt: Option<String>,
    pub consolidation_prompt: Option<String>,
    pub max_iterations: Option<u32>,
    pub debug_enabled: Option<bool>,
    pub upstream: Timeouts,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Model {
    #[serde(flatten)]
    pub settings: Overrides,
    pub agents: BTreeMap<String, Overrides>,
    pub agent_models: Option<BTreeMap<String, Candidate>>,
    pub generated_agent_default: Option<Assignment>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct Routing {
    pub providers: BTreeMap<String, Provider>,
    pub models: BTreeMap<String, Model>,
    pub agents: BTreeMap<String, Overrides>,
    pub default_model: String,
    pub provider: String,
    pub inference_model: String,
    // Model-specific agent overrides are layered over global agent overrides.
    pub selected_agents: BTreeMap<String, Overrides>,
    pub agent_models: BTreeMap<String, Candidate>,
    pub generated_agent_default: Assignment,
}

impl Overrides {
    fn load_prompts(&mut self, path: &std::path::Path) -> Result<(), String> {
        for field in [&mut self.creation_prompt, &mut self.consolidation_prompt] {
            if let Some(name) = field {
                let file = path
                    .parent()
                    .unwrap_or(std::path::Path::new("."))
                    .join(&*name);
                let text = std::fs::read_to_string(&file)
                    .map_err(|e| format!("{}: {e}", file.display()))?;
                if text.trim().is_empty() {
                    return Err(format!("empty prompt {}", file.display()));
                }
                *name = text;
            }
        }
        Ok(())
    }

    pub fn apply(&self, config: &mut Config) -> Result<(), String> {
        if let Some(provider) = &self.provider {
            config.routing.provider = provider.clone();
        }
        if let Some(model) = &self.model {
            if model.trim().is_empty() {
                return Err("inference model must not be empty".into());
            }
            config.routing.inference_model = model.clone();
        }
        if let Some(v) = self.personality_transparency {
            config.personality_transparency = v;
        }
        if let Some(v) = self.creation_policy {
            config.creation.policy = v;
        }
        if let Some(v) = &self.creation_prompt {
            config.creation.prompt = v.clone();
        }
        if let Some(v) = &self.consolidation_prompt {
            config.creation.consolidation_prompt = v.clone();
        }
        if let Some(v) = self.max_iterations {
            if v == 0 {
                return Err("max_iterations must be positive".into());
            }
            config.max_iterations = v;
        }
        if let Some(v) = self.debug_enabled {
            config.debug_enabled = v;
        }
        macro_rules! positive {
            ($($field:ident),*) => { $(if let Some(v) = self.upstream.$field {
                if v == 0 { return Err(concat!(stringify!($field), " must be positive").into()); }
                config.upstream.$field = v;
            })* };
        }
        positive!(
            connect_timeout_ms,
            first_response_timeout_ms,
            idle_timeout_ms,
            total_timeout_ms,
            max_attempts
        );
        if let Some(v) = self.upstream.retry_base_ms {
            config.upstream.retry_base_ms = v;
        }
        if config.upstream.total_timeout_ms > 1_800_000 {
            return Err("total_timeout_ms exceeds 30-minute cap".into());
        }
        Ok(())
    }
}

impl Routing {
    pub fn load_prompts(mut self, path: &std::path::Path) -> Result<Self, String> {
        for agent in self.agents.values_mut() {
            agent.load_prompts(path)?;
        }
        for model in self.models.values_mut() {
            model.settings.load_prompts(path)?;
            for agent in model.agents.values_mut() {
                agent.load_prompts(path)?;
            }
        }
        Ok(self)
    }
}

impl Config {
    pub fn initialize_models(&mut self) -> Result<(), String> {
        for provider in self.routing.providers.values() {
            provider.recovery.validate()?;
            if provider
                .max_concurrent_requests
                .is_some_and(|n| n == 0 || n > tokio::sync::Semaphore::MAX_PERMITS)
            {
                return Err(
                    "max_concurrent_requests must be positive and within semaphore capacity".into(),
                );
            }
        }
        if self.routing.providers.is_empty() {
            self.routing.providers.insert(
                "default".into(),
                Provider {
                    base_url: self.upstream_base_url.clone(),
                    api_key_env: Some("OLLAMA_CLOUD_API_KEY".into()),
                    structured_output: false,
                    ..Default::default()
                },
            );
        }
        if self.routing.provider.is_empty() {
            if self.routing.providers.contains_key("default") {
                self.routing.provider = "default".into();
            } else if self.routing.providers.len() == 1 {
                self.routing.provider = self.routing.providers.keys().next().unwrap().clone();
            }
        }
        if self.routing.inference_model.is_empty() {
            self.routing.inference_model = self.model.clone();
        }
        if self.routing.models.is_empty() {
            self.routing
                .models
                .insert(self.model.clone(), Model::default());
        }
        if self.routing.default_model.is_empty() {
            if self.routing.models.contains_key(&self.model) {
                self.routing.default_model = self.model.clone();
            } else if self.routing.models.len() == 1 {
                self.routing.default_model = self.routing.models.keys().next().unwrap().clone();
            } else {
                return Err("default_model is required with multiple exposed models".into());
            }
        }
        self.model = self.routing.default_model.clone();
        if !self.routing.models.contains_key(&self.model) {
            return Err("default_model is not defined in models".into());
        }
        for id in self.routing.models.keys() {
            if id.trim().is_empty() {
                return Err("exposed model ID must not be empty".into());
            }
            let selected = self.select_model(Some(id))?;
            for candidate in selected.routing.agent_models.values() {
                if candidate.model.trim().is_empty()
                    || candidate.context_tokens == 0
                    || candidate.capabilities.is_empty()
                    || candidate.reasoning.trim().is_empty()
                {
                    return Err("agent_models entries require model, positive context_tokens, capabilities and reasoning".into());
                }
                let mut assigned = selected.clone();
                assigned.routing.provider = candidate.provider.clone();
                assigned.resolve_provider()?;
            }
            let mut default = selected.clone();
            if let Some(provider) = &selected.routing.generated_agent_default.provider {
                default.routing.provider = provider.clone();
            }
            if selected
                .routing
                .generated_agent_default
                .model
                .as_ref()
                .is_some_and(|s| s.trim().is_empty())
            {
                return Err("generated_agent_default.model must not be empty".into());
            }
            default.resolve_provider()?;
            for (name, settings) in self
                .routing
                .agents
                .iter()
                .chain(selected.routing.selected_agents.iter())
            {
                if name.trim().is_empty() {
                    return Err("agent selector must not be empty".into());
                }
                if settings.personality_transparency.is_some()
                    || settings.creation_policy.is_some()
                    || settings.creation_prompt.is_some()
                    || settings.consolidation_prompt.is_some()
                    || settings.debug_enabled.is_some()
                    || settings.max_iterations.is_some()
                {
                    return Err("agent overrides support provider, model and upstream budgets; set request-level operational settings on the exposed model".into());
                }
                selected.for_agent(name, name, None, None)?;
            }
        }
        Ok(())
    }

    pub fn select_model(&self, model: Option<&str>) -> Result<Self, String> {
        let id = model.unwrap_or(&self.model);
        if self.routing.models.is_empty() {
            if id != self.model {
                return Err(format!("unknown model: {id}"));
            }
            return Ok(self.clone());
        }
        let definition = self
            .routing
            .models
            .get(id)
            .ok_or_else(|| format!("unknown model: {id}"))?;
        let mut resolved = self.clone();
        resolved.model = id.into();
        definition.settings.apply(&mut resolved)?;
        resolved.routing.selected_agents = definition.agents.clone();
        if let Some(catalog) = &definition.agent_models {
            resolved.routing.agent_models = catalog.clone();
        }
        if let Some(default) = &definition.generated_agent_default {
            if default.provider.is_some() {
                resolved.routing.generated_agent_default.provider = default.provider.clone();
            }
            if default.model.is_some() {
                resolved.routing.generated_agent_default.model = default.model.clone();
            }
        }
        resolved.resolve_provider()?;
        Ok(resolved)
    }

    pub fn resolve_provider(&mut self) -> Result<(), String> {
        if self.routing.providers.is_empty() {
            return Ok(());
        }
        let provider = self
            .routing
            .providers
            .get(&self.routing.provider)
            .ok_or_else(|| format!("unknown or missing provider: {}", self.routing.provider))?;
        let url =
            reqwest::Url::parse(&provider.base_url).map_err(|_| "invalid provider base_url")?;
        if !["http", "https"].contains(&url.scheme())
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(
                "provider base_url must be HTTP(S), without credentials, query or fragment".into(),
            );
        }
        self.upstream_base_url = provider.base_url.trim_end_matches('/').into();
        self.api_key = match &provider.api_key_env {
            Some(name) => std::env::var(name)
                .map_err(|_| format!("missing provider credential environment variable {name}"))?,
            None => String::new(),
        };
        Ok(())
    }

    pub fn for_agent(
        &self,
        actor: &str,
        name: &str,
        definition_model: Option<&str>,
        definition_provider: Option<&str>,
    ) -> Result<Self, String> {
        let mut config = self.clone();
        if let Some(model) = definition_model.filter(|s| !s.is_empty()) {
            config.routing.inference_model = model.into();
        }
        if let Some(provider) = definition_provider.filter(|s| !s.is_empty()) {
            config.routing.provider = provider.into();
        }
        // UUID wins over name; selected-model declarations override global declarations.
        for map in [&self.routing.agents, &self.routing.selected_agents] {
            if let Some(settings) = map.get(actor).or_else(|| map.get(name)) {
                settings.apply(&mut config)?;
            }
        }
        config.resolve_provider()?;
        Ok(config)
    }

    pub fn inference_model(&self) -> &str {
        if self.routing.inference_model.is_empty() {
            &self.model
        } else {
            &self.routing.inference_model
        }
    }

    pub fn client_scope(&self, client: &str) -> String {
        if self.routing.models.is_empty() {
            client.into()
        } else {
            json!([client, self.model]).to_string()
        }
    }

    pub fn agent_model_contract(&self) -> String {
        format!(
            "\nAGENT MODEL ASSIGNMENT\nThis is a separate administrator-supplied catalog for assigning inference models to generated or revised agents, NOT the client-visible model list. Catalog metadata is guidance, not verified benchmarks. Foreman chooses the model requirements and preferred catalog_id during creation; the dedicated generator creates/registers the agent and follows that selection. Evaluator owns evidence-backed reassignment on revision. Consider capabilities, context_tokens and reasoning. Do not invent providers, model IDs or capability claims. Optional agent-definition field: model_choice={{\"catalog_id\":\"an ID below\",\"reason\":\"why this model fits this agent\"}}. Foreman: include that exact model_choice in creation requirements when selecting. Generator: honor Foreman's choice; if no choice was specified, omit model_choice to use the configured generated-agent default. Evaluator: omit model_choice to preserve an existing assignment on revision, or select a catalog entry with reason. With no catalog do not emit model_choice. Catalog: {}. Default generated-agent assignment (unset fields inherit the selected exposed model): {}.\n",
            json!(self.routing.agent_models),
            json!(self.routing.generated_agent_default)
        )
    }

    pub fn assign_generated(
        &self,
        value: &Value,
        existing: Option<&Value>,
    ) -> Result<Assignment, String> {
        if let Some(choice) = value.get("model_choice").filter(|v| !v.is_null()) {
            let id = choice["catalog_id"]
                .as_str()
                .ok_or("model_choice requires catalog_id")?;
            if choice["reason"]
                .as_str()
                .is_none_or(|s| s.trim().is_empty())
            {
                return Err("model_choice requires reason".into());
            }
            let candidate = self
                .routing
                .agent_models
                .get(id)
                .ok_or("model_choice is not in the configured agent catalog")?;
            return Ok(Assignment {
                provider: Some(candidate.provider.clone()),
                model: Some(candidate.model.clone()),
            });
        }
        if value.get("provider").is_some() || value.get("model").is_some() {
            return Err(
                "generated assignments must use model_choice, not arbitrary provider/model fields"
                    .into(),
            );
        }
        Ok(match existing {
            Some(before) => Assignment {
                provider: before["provider"].as_str().map(str::to_owned),
                model: before["model"].as_str().map(str::to_owned),
            },
            None => self.routing.generated_agent_default.clone(),
        })
    }
}

pub fn with_config(state: &Arc<AppState>, config: Config) -> Result<Arc<AppState>, String> {
    let http = if config.upstream.connect_timeout_ms != state.config.upstream.connect_timeout_ms {
        reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_millis(
                config.upstream.connect_timeout_ms,
            ))
            .build()
            .map_err(|e| e.to_string())?
    } else {
        state.http.clone()
    };
    Ok(Arc::new(AppState {
        config: Arc::new(config),
        http,
        portfolio: state.portfolio.clone(),
        providers: state.providers.clone(),
        external_api_enabled: state.external_api_enabled,
        auth: state.auth.clone(),
    }))
}

pub fn initialize(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let has_provider = conn
        .prepare("PRAGMA table_info(agents)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|s| s == "provider");
    if !has_provider {
        conn.execute_batch("ALTER TABLE agents ADD COLUMN provider TEXT NOT NULL DEFAULT '';")?;
    }
    conn.execute_batch("DROP TRIGGER IF EXISTS revision_agents_UPDATE;
        CREATE TRIGGER revision_agents_UPDATE AFTER UPDATE OF name,purpose,system_prompt,model,provider,retired ON agents BEGIN UPDATE registry_revision SET value=value+1 WHERE id=1; END;")?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS model_migrations(name TEXT PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS run_model_config(run_id TEXT PRIMARY KEY,config TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS call_model_config(run_id TEXT NOT NULL,actor TEXT NOT NULL,config TEXT NOT NULL,PRIMARY KEY(run_id,actor));")?;
    let migrated: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM model_migrations WHERE name='agent_inheritance')",
        [],
        |r| r.get(0),
    )?;
    if !migrated {
        conn.execute_batch("BEGIN; UPDATE agents SET model=''; INSERT INTO model_migrations VALUES('agent_inheritance'); COMMIT;")?;
    }
    Ok(())
}

impl crate::portfolio::Portfolio {
    pub fn migrate_model_preferences(&self, default: &str) -> Result<(), String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let migrated: bool = tx
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM model_migrations WHERE name='client_model_scope')",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if !migrated {
            for table in ["client_creation_policy", "client_personality"] {
                tx.execute(
                    &format!("UPDATE {table} SET client=json_array(client,?1)"),
                    [default],
                )
                .map_err(|e| e.to_string())?;
            }
            let rows = tx
                .prepare("SELECT request_id,context FROM request_creation_policy")
                .map_err(|e| e.to_string())?
                .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            for (request, text) in rows {
                let mut context: crate::policy::Context =
                    serde_json::from_str(&text).map_err(|e| e.to_string())?;
                context.client = json!([context.client, default]).to_string();
                tx.execute(
                    "UPDATE request_creation_policy SET context=?2 WHERE request_id=?1",
                    params![request, json!(context).to_string()],
                )
                .map_err(|e| e.to_string())?;
            }
            tx.execute(
                "INSERT INTO model_migrations VALUES('client_model_scope')",
                [],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())
    }

    pub fn model_snapshot(&self, run: &str) -> Result<Option<Config>, String> {
        let text: Option<String> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT config FROM run_model_config WHERE run_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![run,self.workforce_scope(),self.key_scope()],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        text.map(|s| serde_json::from_str(&s).map_err(|e| e.to_string()))
            .transpose()
    }

    pub fn save_model_snapshot(&self, run: &str, config: &Config) -> Result<(), String> {
        self.conn
            .lock()
            .unwrap()
            .execute(
                "INSERT OR IGNORE INTO run_model_config(run_id,config,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4)",
                params![run, json!(config).to_string(),self.workforce_scope(),self.key_scope()],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn call_config(&self, run: &str, actor: &str, config: &Config) -> Result<Config, String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO call_model_config(run_id,actor,config,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5)",
            params![run, actor, json!(config).to_string(),self.workforce_scope(),self.key_scope()],
        )
        .map_err(|e| e.to_string())?;
        let text: String = conn
            .query_row(
                "SELECT config FROM call_model_config WHERE run_id=?1 AND actor=?2 AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4)",
                params![run, actor,self.workforce_scope(),self.key_scope()],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        let mut saved: Config = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        saved.api_key = config.api_key.clone();
        saved.resolve_provider()?;
        Ok(saved)
    }
}

pub fn restore(state: &Arc<AppState>, mut config: Config) -> Result<Arc<AppState>, String> {
    config.api_key = state.config.api_key.clone();
    config.resolve_provider()?;
    with_config(state, config)
}
