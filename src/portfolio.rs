//! Portfolio: persistent registry of agents CCM has available or created.
//! Neutral machinery — it stores and retrieves whatever CCM decides to
//! create; it never encodes decisions of its own.

use rusqlite::{Connection, OptionalExtension};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Agent {
    #[serde(default)]
    pub provider: String,
    pub id: String,
    pub name: String,
    pub purpose: String,
    pub system_prompt: String,
    pub model: String,
}

/// A not-yet-persisted agent definition (as produced by a CCM decision).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct NewAgent {
    #[serde(default)]
    pub provider: String,
    pub name: String,
    pub purpose: String,
    pub system_prompt: String,
    pub model: String,
}

/// Weak shared backing for short-lived, owner-scoped async handoff cleanup views.
/// The expiry task must not keep a Portfolio, Store, or connection alive between
/// timer wakeups; those resources are upgraded only while cleanup is executing.
pub(crate) struct AsyncRunGateContext {
    conn: Weak<Mutex<Connection>>,
    delivery_budget: Weak<crate::memory::Budget>,
    trace_epoch: String,
    generation: Weak<AtomicU64>,
    trace_bus: tokio::sync::broadcast::Sender<()>,
}

impl AsyncRunGateContext {
    fn new(
        conn: &Arc<Mutex<Connection>>,
        delivery_budget: &Arc<crate::memory::Budget>,
        trace_epoch: String,
        generation: &Arc<AtomicU64>,
        trace_bus: tokio::sync::broadcast::Sender<()>,
    ) -> Self {
        Self {
            conn: Arc::downgrade(conn),
            delivery_budget: Arc::downgrade(delivery_budget),
            trace_epoch,
            generation: Arc::downgrade(generation),
            trace_bus,
        }
    }

    pub(crate) fn cleanup_view(
        self: &Arc<Self>,
        async_ops: Arc<crate::async_ops::AsyncSupervisor>,
    ) -> Result<Arc<Portfolio>, String> {
        Ok(Arc::new(Portfolio {
            conn: self
                .conn
                .upgrade()
                .ok_or("async trace database is unavailable")?,
            // Cleanup writes use the supervisor's explicit persistence fence;
            // this ephemeral view must not own or stop the process Store.
            storage: None,
            delivery_budget: self
                .delivery_budget
                .upgrade()
                .ok_or("async delivery budget is unavailable")?,
            trace_epoch: self.trace_epoch.clone(),
            generation: self
                .generation
                .upgrade()
                .ok_or("async generation state is unavailable")?,
            trace_bus: self.trace_bus.clone(),
            async_ops,
            workforce_id: None,
            owner_key_id: None,
            auth: None,
            run_gate_context: self.clone(),
        }))
    }
}

pub struct Portfolio {
    pub(crate) conn: Arc<Mutex<Connection>>,
    pub(crate) storage: Option<Arc<crate::storage::Store>>,
    pub(crate) delivery_budget: Arc<crate::memory::Budget>,
    pub(crate) trace_epoch: String,
    generation: Arc<AtomicU64>,
    pub(crate) trace_bus: tokio::sync::broadcast::Sender<()>,
    /// Shared process-local async supervisor; scoped portfolio views share it,
    /// while each operation retains the exact authenticated owner context.
    pub async_ops: Arc<crate::async_ops::AsyncSupervisor>,
    pub(crate) run_gate_context: Arc<AsyncRunGateContext>,
    workforce_id: Option<String>,
    owner_key_id: Option<String>,
    pub(crate) auth: Option<crate::auth::AuthContext>,
}

impl Portfolio {
    pub fn read_view(self: &Arc<Self>) -> Result<Arc<Self>, String> {
        let Some(store) = &self.storage else {
            return Ok(self.clone());
        };
        let guard = self.conn.lock().unwrap();
        let conn = store.read_connection()?;
        drop(guard);
        Ok(Arc::new(Self {
            conn: Arc::new(Mutex::new(conn)),
            storage: None,
            delivery_budget: self.delivery_budget.clone(),
            trace_epoch: self.trace_epoch.clone(),
            generation: self.generation.clone(),
            trace_bus: self.trace_bus.clone(),
            async_ops: self.async_ops.clone(),
            run_gate_context: self.run_gate_context.clone(),
            workforce_id: self.workforce_id.clone(),
            owner_key_id: self.owner_key_id.clone(),
            auth: self.auth.clone(),
        }))
    }

    /// Bind the shared database to a verified workforce/key scope. Writes and
    /// reads still use the common portfolio; tenant ownership is enforced by its
    /// scoped query methods rather than by copying or splitting database files.
    pub fn with_scope(self: &Arc<Self>, context: &crate::auth::AuthContext) -> Arc<Self> {
        Arc::new(Self {
            conn: self.conn.clone(),
            storage: self.storage.clone(),
            delivery_budget: self.delivery_budget.clone(),
            trace_epoch: self.trace_epoch.clone(),
            generation: self.generation.clone(),
            trace_bus: self.trace_bus.clone(),
            async_ops: self.async_ops.clone(),
            run_gate_context: self.run_gate_context.clone(),
            workforce_id: Some(context.workforce_id.clone()),
            owner_key_id: Some(context.key_id.clone()),
            auth: Some(context.clone()),
        })
    }

    pub fn workforce_scope(&self) -> Option<&str> {
        self.workforce_id.as_deref()
    }

    pub fn key_scope(&self) -> Option<&str> {
        self.owner_key_id.as_deref()
    }

    #[cfg(test)]
    pub fn open(path: &str) -> rusqlite::Result<Self> {
        Self::open_budget(path, crate::config::MemoryConfig::default().bytes())
    }

    pub fn open_budget(path: &str, budget: usize) -> rusqlite::Result<Self> {
        let (conn, storage) = if path == ":memory:" {
            (Connection::open_in_memory()?, None)
        } else {
            let (conn, store) =
                crate::storage::connection(std::path::Path::new(path), budget / 2 - 1024 * 1024)
                    .map_err(rusqlite::Error::InvalidParameterName)?;
            (conn, Some(store))
        };
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS agents (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                purpose TEXT NOT NULL,
                system_prompt TEXT NOT NULL,
                model TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                usage_count INTEGER NOT NULL DEFAULT 0,
                last_used_at INTEGER
            );",
        )?;
        crate::operations::initialize(&conn)?;
        crate::observe::initialize(&conn)?;
        crate::workforce::initialize(&conn)?;
        crate::improvement::initialize(&conn)?;
        crate::policy::initialize(&conn)?;
        crate::model_config::initialize(&conn)?;
        crate::auth::initialize(&conn)?;
        crate::observe::archive_definitions(&conn)?;
        let conn = Arc::new(Mutex::new(conn));
        let delivery_budget = crate::memory::Budget::new(budget / 4);
        let async_ops = Arc::new(
            crate::async_ops::AsyncSupervisor::open(
                conn.clone(),
                storage.clone(),
                delivery_budget.clone(),
            )
            .map_err(rusqlite::Error::InvalidParameterName)?,
        );
        let trace_epoch = uuid::Uuid::new_v4().simple().to_string();
        let generation = Arc::new(AtomicU64::new(0));
        let trace_bus = tokio::sync::broadcast::channel(256).0;
        let run_gate_context = Arc::new(AsyncRunGateContext::new(
            &conn,
            &delivery_budget,
            trace_epoch.clone(),
            &generation,
            trace_bus.clone(),
        ));
        let portfolio = Self {
            conn,
            storage,
            delivery_budget,
            trace_epoch,
            generation,
            trace_bus,
            async_ops: async_ops.clone(),
            run_gate_context: run_gate_context.clone(),
            workforce_id: None,
            owner_key_id: None,
            auth: None,
        };
        async_ops
            .reconcile_startup_handoff_cleanups(&run_gate_context)
            .map_err(rusqlite::Error::InvalidParameterName)?;
        async_ops
            .start_recovered_handoff_expiry_worker(Arc::downgrade(&run_gate_context))
            .map_err(rusqlite::Error::InvalidParameterName)?;
        Ok(portfolio)
    }

    pub fn list(&self) -> rusqlite::Result<Vec<Agent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, purpose, system_prompt, model, provider FROM agents
             WHERE retired = 0 AND (?1 IS NULL OR workforce_id=?1) ORDER BY created_at",
        )?;
        stmt.query_map([self.workforce_id.as_deref()], row_to_agent)?
            .collect()
    }

    pub fn get(&self, id: &str) -> rusqlite::Result<Option<Agent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT id, name, purpose, system_prompt, model, provider FROM agents WHERE id = ?1 AND retired = 0 AND (?2 IS NULL OR workforce_id=?2)")?;
        stmt.query_row(
            rusqlite::params![id, self.workforce_id.as_deref()],
            row_to_agent,
        )
        .optional()
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }

    #[cfg(test)]
    pub fn insert(&self, new: &NewAgent) -> rusqlite::Result<Agent> {
        self.insert_if_current(new, self.generation())?
            .ok_or(rusqlite::Error::InvalidQuery)
    }

    /// A reset invalidates creation requests that started in an older generation.
    #[cfg(test)]
    pub fn insert_if_current(
        &self,
        new: &NewAgent,
        generation: u64,
    ) -> rusqlite::Result<Option<Agent>> {
        let conn = self.conn.lock().unwrap();
        if self.generation() != generation {
            return Ok(None);
        }
        let id = uuid::Uuid::new_v4().to_string();
        let now = now_unix();
        conn.execute(
            "INSERT INTO agents (id, name, purpose, system_prompt, model, created_at, usage_count, provider, workforce_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8)",
            rusqlite::params![id, new.name, new.purpose, new.system_prompt, new.model, now, new.provider, self.workforce_id.as_deref()],
        )?;
        Ok(Some(Agent {
            provider: new.provider.clone(),
            id,
            name: new.name.clone(),
            purpose: new.purpose.clone(),
            system_prompt: new.system_prompt.clone(),
            model: new.model.clone(),
        }))
    }

    pub fn record_usage(&self, id: &str) {
        let conn = self.conn.lock().unwrap();
        let _ = conn.execute(
            "UPDATE agents SET usage_count = usage_count + 1, last_used_at = ?2 WHERE id = ?1",
            rusqlite::params![id, now_unix()],
        );
    }

    /// Removes every agent. Returns how many were removed.
    #[cfg(test)]
    pub fn delete_all(&self) -> rusqlite::Result<usize> {
        Ok(self.reset()?["agents"].as_u64().unwrap() as usize)
    }

    pub fn reset(&self) -> rusqlite::Result<serde_json::Value> {
        // The legacy reset contract is global. A scoped request must never turn
        // that operation into a cross-workforce destructive action.
        if self.workforce_id.is_some() {
            return Err(rusqlite::Error::InvalidQuery);
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        crate::observe::archive_definitions(&tx)?;
        let agents = tx.execute("DELETE FROM agents", [])?;
        let workflows = tx.execute("DELETE FROM workflows", [])?;
        let runs = tx.execute("DELETE FROM workflow_runs", [])?;
        tx.execute("DELETE FROM agent_continuations", [])?;
        tx.execute("DELETE FROM consolidation_aliases", [])?;
        let now = crate::observe::now_ms();
        tx.execute("UPDATE improvement_epoch SET value=value+1 WHERE id=1", [])?;
        tx.execute(
            "UPDATE improvement_checkpoints SET status='cancelled',outcome=?1,updated_ms=?2
             WHERE status IN ('queued','running','triggered')",
            rusqlite::params![
                serde_json::json!({"reason":"workforce reset invalidated checkpoint epoch"})
                    .to_string(),
                now
            ],
        )?;
        tx.execute("UPDATE improvement_jobs SET status='cancelled',updated_ms=?1,outcome='{\"reason\":\"workforce reset\"}' WHERE status IN ('held','queued','running')",[now])?;
        tx.execute("UPDATE trace_runs SET status='reset',updated_ms=?1 WHERE status IN ('running','paused','waiting_tools')",[now])?;
        tx.execute(
            "UPDATE trace_spans SET status='reset',ended_ms=?1 WHERE ended_ms IS NULL",
            [now],
        )?;
        tx.execute(
            "UPDATE trace_tools SET status='reset' WHERE status='waiting'",
            [],
        )?;
        tx.execute(
            "UPDATE trace_controls SET status='not_delivered' WHERE status='queued'",
            [],
        )?;
        tx.commit()?;
        self.generation.fetch_add(1, Ordering::SeqCst);
        drop(conn);
        let _ = self.trace_emit(
            "system",
            None,
            "system.reset",
            &serde_json::json!({"agents":agents,"workflows":workflows,"history_retained":true}),
        );
        Ok(serde_json::json!({"agents":agents,"workflows":workflows,"workflow_runs":runs}))
    }

    /// Free-text listing handed to the evaluation stage — the model decides
    /// fit; we only present what exists.
    pub fn listing(&self) -> rusqlite::Result<String> {
        let agents = self.list()?;
        if agents.is_empty() {
            return Ok("(the portfolio is empty — no agents exist yet)".to_string());
        }
        Ok(agents
            .iter()
            .map(|a| {
                format!(
                    "- id={} | name={} | purpose={}\n  system_prompt: {}",
                    a.id, a.name, a.purpose, a.system_prompt
                )
            })
            .collect::<Vec<_>>()
            .join("\n"))
    }
}

impl Drop for Portfolio {
    fn drop(&mut self) {
        if let Some(store) = &self.storage {
            if let Err(error) = store.stop() {
                tracing::error!(%error, "database shutdown flush failed");
            }
        }
    }
}

fn row_to_agent(row: &rusqlite::Row) -> rusqlite::Result<Agent> {
    Ok(Agent {
        provider: row.get(5)?,
        id: row.get(0)?,
        name: row.get(1)?,
        purpose: row.get(2)?,
        system_prompt: row.get(3)?,
        model: row.get(4)?,
    })
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn portfolio() -> Portfolio {
        Portfolio::open(":memory:").unwrap()
    }

    #[test]
    fn empty_portfolio_lists_nothing() {
        let p = portfolio();
        assert!(p.list().unwrap().is_empty());
        assert!(p.listing().unwrap().contains("empty"));
    }

    #[test]
    fn insert_and_get_roundtrip() {
        let p = portfolio();
        let new = NewAgent {
            provider: String::new(),
            name: "rust-helper".into(),
            purpose: "answers Rust questions".into(),
            system_prompt: "You are a Rust expert.".into(),
            model: "deepseek-v4.1-flash".into(),
        };
        let stored = p.insert(&new).unwrap();
        let fetched = p.get(&stored.id).unwrap().unwrap();
        assert_eq!(fetched.name, "rust-helper");
        assert_eq!(fetched.system_prompt, "You are a Rust expert.");
        assert_eq!(p.list().unwrap().len(), 1);
        assert!(p.listing().unwrap().contains("rust-helper"));
        assert!(p.listing().unwrap().contains("You are a Rust expert."));
    }

    #[test]
    fn storage_failure_is_not_an_empty_portfolio() {
        let p = portfolio();
        p.conn
            .lock()
            .unwrap()
            .execute("DROP TABLE agents", [])
            .unwrap();
        assert!(p.list().is_err());
        assert!(p.listing().is_err());
        assert!(p.get("missing").is_err());
    }

    #[test]
    fn absent_agent_is_not_a_storage_error() {
        assert!(portfolio().get("missing").unwrap().is_none());
    }

    #[test]
    fn reset_invalidates_in_flight_creation() {
        let p = portfolio();
        let generation = p.generation();
        let agent = NewAgent {
            provider: String::new(),
            name: "a".into(),
            purpose: "p".into(),
            system_prompt: "s".into(),
            model: "m".into(),
        };
        p.insert_if_current(&agent, generation).unwrap().unwrap();
        assert_eq!(p.delete_all().unwrap(), 1);
        assert!(p.insert_if_current(&agent, generation).unwrap().is_none());
        assert!(p.list().unwrap().is_empty());
        assert!(
            p.insert_if_current(&agent, p.generation())
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn usage_is_recorded() {
        let p = portfolio();
        let stored = p
            .insert(&NewAgent {
                provider: String::new(),
                name: "a".into(),
                purpose: "p".into(),
                system_prompt: "s".into(),
                model: "m".into(),
            })
            .unwrap();
        p.record_usage(&stored.id);
        p.record_usage(&stored.id);
        let conn = p.conn.lock().unwrap();
        let count: i64 = conn
            .query_row(
                "SELECT usage_count FROM agents WHERE id = ?1",
                [&stored.id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 2);
    }
}
