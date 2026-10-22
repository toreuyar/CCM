//! Persistent operational evidence and generated artifacts, not routing policy.
use crate::portfolio::Portfolio;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    let columns: Vec<String> = conn
        .prepare("PRAGMA table_info(agents)")?
        .query_map([], |r| r.get(1))?
        .collect::<Result<_, _>>()?;
    if !columns.iter().any(|c| c == "retired") {
        conn.execute_batch("ALTER TABLE agents ADD COLUMN retired INTEGER NOT NULL DEFAULT 0;")?;
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS workflows (
        id TEXT PRIMARY KEY, definition TEXT NOT NULL, retired INTEGER NOT NULL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY, request_id TEXT NOT NULL, kind TEXT NOT NULL,
        entity_id TEXT NOT NULL, status TEXT NOT NULL, duration_ms INTEGER NOT NULL,
        usage TEXT, detail TEXT NOT NULL, created_at INTEGER NOT NULL DEFAULT (unixepoch()));
        CREATE TABLE IF NOT EXISTS evaluations (
        id INTEGER PRIMARY KEY, request_id TEXT NOT NULL, entity_id TEXT NOT NULL,
        entity_kind TEXT NOT NULL, verdict TEXT NOT NULL, before_definition TEXT NOT NULL,
        after_definition TEXT, applied INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL DEFAULT (unixepoch()));
        CREATE TABLE IF NOT EXISTS workflow_runs (
        id TEXT PRIMARY KEY, state TEXT NOT NULL, claimed INTEGER NOT NULL DEFAULT 0);",
    )
}

impl Portfolio {
    // Arguments mirror the persisted event schema; missing usage stays distinct from zero.
    #[allow(clippy::too_many_arguments)]
    pub fn event(
        &self,
        request: &str,
        kind: &str,
        entity: &str,
        status: &str,
        duration_ms: u64,
        usage: Option<&Value>,
        detail: &str,
    ) -> Result<(), String> {
        self.conn.lock().unwrap().execute(
            "INSERT INTO events(request_id,kind,entity_id,status,duration_ms,usage,detail,workforce_id,owner_key_id) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![request,kind,entity,status,duration_ms,usage.map(Value::to_string),detail,self.workforce_scope(),self.key_scope()]
        ).map(|_| ()).map_err(|e| e.to_string())
    }

    pub fn statistics(&self) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT kind,entity_id,status,COUNT(*),SUM(duration_ms),
            SUM(json_extract(usage,'$.prompt_tokens')),SUM(json_extract(usage,'$.completion_tokens'))
            FROM events WHERE (?1 IS NULL OR workforce_id=?1) AND (?2 IS NULL OR owner_key_id=?2)
            GROUP BY kind,entity_id,status ORDER BY kind,entity_id,status").map_err(|e|e.to_string())?;
        let rows = stmt.query_map(params![self.workforce_scope(),self.key_scope()], |r| Ok(json!({
            "kind":r.get::<_,String>(0)?,"entity_id":r.get::<_,String>(1)?,"status":r.get::<_,String>(2)?,
            "count":r.get::<_,i64>(3)?,"duration_ms":r.get::<_,i64>(4)?,
            "prompt_tokens":r.get::<_,Option<i64>>(5)?,"completion_tokens":r.get::<_,Option<i64>>(6)?
        }))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        Ok(json!({"groups":rows,"token_usage":"provider-reported; null means unavailable"}))
    }

    pub fn evaluations(&self) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT request_id,entity_id,entity_kind,verdict,before_definition,after_definition,applied FROM evaluations WHERE (?1 IS NULL OR workforce_id=?1) AND (?2 IS NULL OR owner_key_id=?2) ORDER BY id DESC LIMIT 100").map_err(|e|e.to_string())?;
        let rows = stmt
            .query_map(params![self.workforce_scope(), self.key_scope()], |r| {
                Ok(json!({"request_id":r.get::<_,String>(0)?,
                    "entity_id":r.get::<_,String>(1)?,"entity_kind":r.get::<_,String>(2)?,
                    "verdict":r.get::<_,String>(3)?,"before":r.get::<_,String>(4)?,
                    "after":r.get::<_,Option<String>>(5)?,"applied":r.get::<_,bool>(6)?
                }))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        Ok(json!(rows))
    }

    pub fn workflow_list(&self) -> Result<Vec<crate::workflow::Workflow>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT definition FROM workflows WHERE retired=0 AND (?1 IS NULL OR workforce_id=?1) ORDER BY id")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([self.workforce_scope()], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        rows.iter()
            .map(|s| serde_json::from_str(s).map_err(|e| e.to_string()))
            .collect()
    }

    pub fn workflow_get(&self, id: &str) -> Result<Option<crate::workflow::Workflow>, String> {
        let conn = self.conn.lock().unwrap();
        let text: Option<String> = conn
            .query_row(
                "SELECT definition FROM workflows WHERE id=?1 AND retired=0 AND (?2 IS NULL OR workforce_id=?2)",
                params![id,self.workforce_scope()],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        text.map(|s| serde_json::from_str(&s).map_err(|e| e.to_string()))
            .transpose()
    }

    #[cfg(test)]
    pub fn workflow_insert(
        &self,
        workflow: &crate::workflow::Workflow,
        generation: u64,
    ) -> Result<(), String> {
        workflow.validate()?;
        let conn = self.conn.lock().unwrap();
        if self.generation() != generation {
            return Err("portfolio reset during workflow creation".into());
        }
        conn.execute(
            "INSERT INTO workflows(id,definition,workforce_id) VALUES (?1,?2,?3)",
            params![
                workflow.id,
                serde_json::to_string(workflow).map_err(|e| e.to_string())?,
                self.workforce_scope()
            ],
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    pub fn save_run(&self, id: &str, state: &Value, generation: u64) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        if self.generation() != generation {
            return Err("portfolio reset during workflow execution".into());
        }
        conn.execute(
            "INSERT INTO workflow_runs(id,state,claimed,workforce_id,owner_key_id) VALUES (?1,?2,0,?3,?4)
            ON CONFLICT(id) DO UPDATE SET state=excluded.state,claimed=0
            WHERE workflow_runs.workforce_id=excluded.workforce_id AND workflow_runs.owner_key_id=excluded.owner_key_id",
            params![id, state.to_string(),self.workforce_scope(),self.key_scope()],
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
    }

    pub fn claim_run(&self, id: &str) -> Result<Value, String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        if tx
            .execute(
                "UPDATE workflow_runs SET claimed=1 WHERE id=?1 AND claimed=0 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![id,self.workforce_scope(),self.key_scope()],
            )
            .map_err(|e| e.to_string())?
            != 1
        {
            return Err("unknown, completed, or already resumed workflow run".into());
        }
        let text: String = tx
            .query_row("SELECT state FROM workflow_runs WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)", params![id,self.workforce_scope(),self.key_scope()], |r| {
                r.get(0)
            })
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        serde_json::from_str(&text).map_err(|e| e.to_string())
    }

    pub fn finish_run(&self, id: &str) -> Result<(), String> {
        self.conn
            .lock()
            .unwrap()
            .execute("DELETE FROM workflow_runs WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)", params![id,self.workforce_scope(),self.key_scope()])
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
