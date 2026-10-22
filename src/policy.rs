//! Per-client creation authority. No raw API credentials are persisted or traced.
use crate::{observe::now_ms, portfolio::Portfolio};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const FALLBACK_ID: &str = "00000000-0000-4000-8000-000000000001";
pub fn fallback(model: &str) -> crate::portfolio::Agent {
    crate::portfolio::Agent {
        provider: String::new(),
        id: FALLBACK_ID.into(),
        name: "default".into(),
        purpose: "Execute the original client task without a generated specialist definition"
            .into(),
        system_prompt: String::new(),
        model: model.into(),
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CreationPolicy {
    AlwaysPreExecution,
    DefaultPreExecution,
    #[default]
    Dynamic,
    #[serde(alias = "DefaultProstExecution")]
    DefaultPostExecution,
    AlwaysPostExecution,
}
impl CreationPolicy {
    pub fn initial_phase(self) -> Option<&'static str> {
        match self {
            Self::AlwaysPreExecution | Self::DefaultPreExecution => Some("pre"),
            Self::AlwaysPostExecution | Self::DefaultPostExecution => Some("post"),
            Self::Dynamic => None,
        }
    }
    pub fn is_default(self) -> bool {
        matches!(self, Self::DefaultPreExecution | Self::DefaultPostExecution)
    }
    pub fn mutable(self) -> bool {
        self.is_default() || self == Self::Dynamic
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Context {
    pub client: String,
    pub policy: CreationPolicy,
    pub phase: Option<String>,
    pub orders: Vec<Value>,
    #[serde(default)]
    pub locked: bool,
}

pub fn identity(headers: &axum::http::HeaderMap, native: bool) -> String {
    if native {
        return "native".into();
    }
    let key = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.split_once(' '))
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
        .map(|(_, key)| key.trim())
        .filter(|s| !s.is_empty());
    match key {
        None => "anonymous".into(),
        Some(key) => {
            let digest = ring::digest::digest(&ring::digest::SHA256, key.as_bytes());
            format!(
                "key:{}",
                digest
                    .as_ref()
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
            )
        }
    }
}

pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS client_personality(client TEXT PRIMARY KEY,mode TEXT NOT NULL CHECK(mode IN ('introvert','extrovert')),updated_ms INTEGER NOT NULL);")?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS client_creation_policy(client TEXT PRIMARY KEY,policy TEXT NOT NULL,updated_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS request_creation_policy(request_id TEXT PRIMARY KEY,run_id TEXT NOT NULL,context TEXT NOT NULL,flipped INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS agent_continuations(run_id TEXT PRIMARY KEY,definition TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS creation_receipts(job_id TEXT NOT NULL,order_index INTEGER NOT NULL,kind TEXT NOT NULL,entity_id TEXT NOT NULL,PRIMARY KEY(job_id,order_index));
        CREATE TABLE IF NOT EXISTS workforce_plans(job_id TEXT PRIMARY KEY,plan TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS consolidation_aliases(kind TEXT NOT NULL,duplicate_id TEXT PRIMARY KEY,survivor_id TEXT NOT NULL,created_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS registry_revision(id INTEGER PRIMARY KEY,value INTEGER NOT NULL);
        INSERT OR IGNORE INTO registry_revision VALUES(1,0);")?;
    for table in ["agents", "workflows"] {
        for op in ["INSERT", "UPDATE", "DELETE"] {
            let event = if op == "UPDATE" {
                if table == "agents" {
                    "UPDATE OF name,purpose,system_prompt,model,retired"
                } else {
                    "UPDATE OF definition,retired"
                }
            } else {
                op
            };
            conn.execute_batch(&format!("CREATE TRIGGER IF NOT EXISTS revision_{table}_{op} AFTER {event} ON {table} BEGIN UPDATE registry_revision SET value=value+1 WHERE id=1; END;"))?;
        }
    }
    Ok(())
}

impl Portfolio {
    pub fn client_personality(
        &self,
        client: &str,
    ) -> Result<crate::config::PersonalityTransparency, String> {
        let mode: Option<String> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT mode FROM client_personality WHERE client=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![client,self.workforce_scope(),self.key_scope()],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        Ok(if mode.as_deref() == Some("extrovert") {
            crate::config::PersonalityTransparency::Extrovert
        } else {
            crate::config::PersonalityTransparency::Introvert
        })
    }

    pub fn set_client_personality(&self, client: &str, mode: &str) -> Result<(), String> {
        if !["introvert", "extrovert"].contains(&mode) {
            return Err("personality must be introvert or extrovert".into());
        }
        self.conn.lock().unwrap().execute(
            "INSERT INTO client_personality(client,mode,updated_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(client) DO UPDATE SET mode=excluded.mode,updated_ms=excluded.updated_ms,workforce_id=excluded.workforce_id,owner_key_id=excluded.owner_key_id",
            params![client, mode, now_ms(),self.workforce_scope(),self.key_scope()]
        ).map_err(|e| e.to_string())?;
        Ok(())
    }

    pub fn save_continuation_agent(
        &self,
        run: &str,
        agent: &crate::portfolio::Agent,
        generation: u64,
    ) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        if self.generation() != generation {
            return Err("reset before continuation capture".into());
        }
        conn.execute("INSERT INTO agent_continuations(run_id,definition,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4) ON CONFLICT(run_id) DO UPDATE SET definition=excluded.definition WHERE agent_continuations.workforce_id=excluded.workforce_id AND agent_continuations.owner_key_id=excluded.owner_key_id",params![run,json!(agent).to_string(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
        Ok(())
    }
    pub fn continuation_agent(&self, run: &str) -> Result<Option<crate::portfolio::Agent>, String> {
        let text: Option<String> = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT definition FROM agent_continuations WHERE run_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![run,self.workforce_scope(),self.key_scope()],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        text.map(|s| serde_json::from_str(&s))
            .transpose()
            .map_err(|e| e.to_string())
    }
    pub fn validate_policy_owner(&self, payload: &Value, client: &str) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        for message in payload["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .rev()
            .take_while(|m| m["role"] == "tool")
        {
            let text:Option<String>=conn.query_row("SELECT p.context FROM request_creation_policy p JOIN trace_tools t ON p.run_id=t.run_id JOIN trace_runs r ON r.id=t.run_id WHERE t.id=?1 AND (?2 IS NULL OR r.workforce_id=?2) AND (?3 IS NULL OR r.owner_key_id=?3) ORDER BY p.rowid DESC LIMIT 1",params![message["tool_call_id"].as_str().unwrap_or(""),self.workforce_scope(),self.key_scope()],|r|r.get(0)).optional().map_err(|e|e.to_string())?;
            if let Some(text) = text {
                let context: Context = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                if context.client != client {
                    return Err("tool continuation belongs to another client identity".into());
                }
            }
        }
        Ok(())
    }
    pub fn authorize_pre_creation(&self, request: &str) -> Result<(), String> {
        let mut context = self.policy_context(request)?;
        if context.phase.as_deref() != Some("pre") {
            return Err(
                "creation unavailable: resolve pre timing before synchronous creation".into(),
            );
        }
        context.locked = true;
        self.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE request_creation_policy SET flipped=1,context=?2 WHERE request_id=?1",
                params![request, json!(context).to_string()],
            )
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    pub fn bind_policy(
        &self,
        request: &str,
        run: &str,
        client: &str,
        default: CreationPolicy,
    ) -> Result<Context, String> {
        let conn = self.conn.lock().unwrap();
        let previous:Option<String>=conn.query_row("SELECT context FROM request_creation_policy WHERE run_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3) ORDER BY rowid DESC LIMIT 1",params![run,self.workforce_scope(),self.key_scope()],|r|r.get(0)).optional().map_err(|e|e.to_string())?;
        let context = if let Some(text) = previous {
            let context: Context = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            if context.client != client {
                return Err("tool continuation belongs to another client identity".into());
            }
            context
        } else {
            let saved: Option<String> = conn
                .query_row(
                    "SELECT policy FROM client_creation_policy WHERE client=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                    params![client,self.workforce_scope(),self.key_scope()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?;
            let policy = saved
                .map(|s| serde_json::from_str(&s))
                .transpose()
                .map_err(|e| e.to_string())?
                .unwrap_or(default);
            Context {
                client: client.into(),
                policy,
                phase: policy.initial_phase().map(str::to_string),
                orders: vec![],
                locked: false,
            }
        };
        conn.execute("INSERT OR IGNORE INTO request_creation_policy(request_id,run_id,context,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5)",params![request,run,json!(context).to_string(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
        Ok(context)
    }
    pub fn policy_context(&self, request: &str) -> Result<Context, String> {
        let text: String = self
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT context FROM request_creation_policy WHERE request_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![request,self.workforce_scope(),self.key_scope()],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        serde_json::from_str(&text).map_err(|e| e.to_string())
    }
    pub fn change_timing(
        &self,
        request: &str,
        phase: &str,
        quote: &str,
        payload: &Value,
    ) -> Result<Context, String> {
        if !["pre", "post"].contains(&phase) {
            return Err("timing must be pre or post".into());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let (text, flipped): (String, bool) = tx
            .query_row(
                "SELECT context,flipped FROM request_creation_policy WHERE request_id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![request,self.workforce_scope(),self.key_scope()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .map_err(|e| e.to_string())?;
        let mut context: Context = serde_json::from_str(&text).map_err(|e| e.to_string())?;
        if !context.policy.mutable() || context.locked || flipped {
            return Err("creation timing is fixed for this request".into());
        }
        if context.policy.is_default() {
            let user = payload["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .rev()
                .find(|m| m["role"] == "user")
                .map(|m| &m["content"]);
            let quoted = user.is_some_and(|v| {
                v.as_str().is_some_and(|s| s.contains(quote))
                    || v.as_array().is_some_and(|parts| {
                        parts.iter().any(|p| {
                            p["type"] == "text"
                                && p["text"].as_str().is_some_and(|s| s.contains(quote))
                        })
                    })
            });
            if quote.trim().is_empty() || !quoted {
                return Err("persistent timing flip requires a verbatim explicit instruction from the latest user message".into());
            }
            context.policy = if phase == "pre" {
                CreationPolicy::DefaultPreExecution
            } else {
                CreationPolicy::DefaultPostExecution
            };
            tx.execute("INSERT INTO client_creation_policy(client,policy,updated_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(client) DO UPDATE SET policy=excluded.policy,updated_ms=excluded.updated_ms,workforce_id=excluded.workforce_id,owner_key_id=excluded.owner_key_id",params![context.client,json!(context.policy).to_string(),now_ms(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
        }
        context.phase = Some(phase.into());
        context.locked = true;
        tx.execute(
            "UPDATE request_creation_policy SET context=?2,flipped=1 WHERE request_id=?1",
            params![request, json!(context).to_string()],
        )
        .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);
        self.trace_event(
            request,
            "creation.policy_changed",
            &json!({"policy":context.policy,"phase":phase,"user_instruction":quote}),
        )?;
        Ok(context)
    }
    pub fn defer_creation(
        &self,
        request: &str,
        kind: &str,
        requirements: &str,
    ) -> Result<(), String> {
        if !["agent", "workflow"].contains(&kind) || requirements.trim().is_empty() {
            return Err("invalid deferred creation order".into());
        }
        let mut context = self.policy_context(request)?;
        if context.policy != CreationPolicy::Dynamic || context.phase.as_deref() != Some("post") {
            return Err("mandatory deferred orders require Dynamic post timing".into());
        }
        let order = json!({"kind":kind,"requirements":requirements});
        context.locked = true;
        if !context.orders.contains(&order) {
            context.orders.push(order);
        }
        self.conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE request_creation_policy SET context=?2,flipped=1 WHERE request_id=?1",
                params![request, json!(context).to_string()],
            )
            .map_err(|e| e.to_string())?;
        self.trace_event(
            request,
            "creation.deferred",
            &json!({"kind":kind,"requirements":requirements}),
        )
    }
}

pub fn tools(context: &Context) -> Vec<Value> {
    let mut tools = vec![
        json!({"name":"list_agents"}),
        json!({"name":"list_workflows"}),
        json!({"name":"get_statistics"}),
        json!({"name":"get_evaluations"}),
        json!({"name":"evaluate_agent","arguments":{"id":"id","task":"context","result":"evidence"}}),
        json!({"name":"evaluate_workflow","arguments":{"id":"id","task":"context","result":"evidence"}}),
    ];
    if context.policy.mutable() && !context.locked {
        tools.push(json!({"name":"set_creation_timing","arguments":{"timing":"pre|post","user_instruction":"verbatim latest user instruction; required for Default modes"}}));
    }
    if context.phase.as_deref() == Some("pre") {
        tools.extend(creation_tools());
    }
    if context.policy == CreationPolicy::Dynamic && context.phase.as_deref() == Some("post") {
        tools.push(json!({"name":"defer_creation","arguments":{"kind":"agent|workflow","requirements":"mandatory reusable definition requirements"}}));
    }
    tools
}
pub fn creation_tools() -> Vec<Value> {
    vec![
        json!({"name":"create_agent","arguments":{"requirements":"reusable competence"}}),
        json!({"name":"create_workflow","arguments":{"requirements":"executable workflow requirements"}}),
    ]
}
