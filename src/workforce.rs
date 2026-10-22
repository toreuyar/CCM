//! Generated-workforce inventory, timestamped usage and operational retirement.
use crate::{observe::now_ms, portfolio::Portfolio};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

pub const RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;

pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    let columns = conn
        .prepare("PRAGMA table_info(agents)")?
        .query_map([], |r| r.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|c| c == "retired_ms") {
        conn.execute_batch("ALTER TABLE agents ADD COLUMN retired_ms INTEGER;")?;
        // Existing retirement evidence is authoritative; otherwise begin retention now.
        conn.execute("UPDATE agents SET retired_ms=COALESCE((SELECT MAX(created_at)*1000 FROM evaluations WHERE entity_id=agents.id AND applied=1 AND json_extract(verdict,'$.action')='retire'),?1) WHERE retired=1", [now_ms()])?;
    }
    conn.execute_batch("CREATE TABLE IF NOT EXISTS workforce_usage(
        entity_id TEXT NOT NULL, kind TEXT NOT NULL, invocation_key TEXT NOT NULL,
        request_id TEXT NOT NULL, at_ms INTEGER NOT NULL,
        PRIMARY KEY(entity_id,kind,invocation_key));
        CREATE INDEX IF NOT EXISTS workforce_usage_time ON workforce_usage(entity_id,at_ms);
        CREATE TABLE IF NOT EXISTS workforce_metadata(key TEXT PRIMARY KEY,value INTEGER NOT NULL);")?;
    conn.execute(
        "INSERT OR IGNORE INTO workforce_metadata VALUES('tracking_since_ms',?1)",
        [now_ms()],
    )?;
    conn.execute_batch(
        "CREATE TRIGGER IF NOT EXISTS agent_retirement_time AFTER UPDATE OF retired ON agents
        WHEN NEW.retired=1 AND OLD.retired=0 BEGIN
        UPDATE agents SET retired_ms=CAST(strftime('%s','now') AS INTEGER)*1000 WHERE id=NEW.id;
        END;",
    )?;
    Ok(())
}

impl Portfolio {
    /// Idempotent assignment keys survive tool continuation; attempt keys do not.
    pub fn workforce_usage(
        &self,
        entity: &str,
        kind: &str,
        key: &str,
        request: &str,
    ) -> Result<(), String> {
        self.conn
            .lock()
            .unwrap()
            .execute(
                "INSERT OR IGNORE INTO workforce_usage(entity_id,kind,invocation_key,request_id,at_ms,workforce_id) VALUES(?1,?2,?3,?4,?5,?6)",
                params![entity, kind, key, request, now_ms(),self.workforce_scope()],
            )
            .map_err(|e| e.to_string())?;
        let _ = self.trace_bus.send(());
        Ok(())
    }

    pub fn purge_retired_agents(&self, now: i64) -> Result<usize, String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let due: i64 = tx.query_row("SELECT COUNT(*) FROM agents WHERE retired=1 AND retired_ms IS NOT NULL AND retired_ms<=?1",[now-RETENTION_MS],|r|r.get(0)).map_err(|e|e.to_string())?;
        if due == 0 {
            return Ok(0);
        }
        // Immutable snapshots, evaluations, usage and execution evidence are retained.
        crate::observe::archive_definitions(&tx).map_err(|e| e.to_string())?;
        let removed = tx
            .execute(
                "DELETE FROM agents WHERE retired=1 AND retired_ms IS NOT NULL AND retired_ms<=?1",
                [now - RETENTION_MS],
            )
            .map_err(|e| e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);
        if removed > 0 {
            let _ = self.trace_bus.send(());
        }
        Ok(removed)
    }

    pub fn workforce(&self, now: i64) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id,name,purpose,system_prompt,model,retired,retired_ms,created_at,provider FROM agents WHERE (?1 IS NULL OR workforce_id=?1) ORDER BY name,id").map_err(|e| e.to_string())?;
        let mut entities = stmt.query_map([self.workforce_scope()], |r| Ok(json!({
            "id":r.get::<_,String>(0)?,"kind":"agent","name":r.get::<_,String>(1)?,
            "definition":{"id":r.get::<_,String>(0)?,"name":r.get::<_,String>(1)?,"purpose":r.get::<_,String>(2)?,"system_prompt":r.get::<_,String>(3)?,"model":r.get::<_,String>(4)?,"provider":r.get::<_,String>(8)?},
            "retired":r.get::<_,bool>(5)?,"retired_ms":r.get::<_,Option<i64>>(6)?,"created_ms":r.get::<_,i64>(7)?*1000
        }))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        drop(stmt);
        let mut stmt = conn
            .prepare("SELECT id,definition,retired FROM workflows WHERE (?1 IS NULL OR workforce_id=?1) ORDER BY id")
            .map_err(|e| e.to_string())?;
        let workflows = stmt
            .query_map([self.workforce_scope()], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, bool>(2)?,
                ))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        drop(stmt);
        for (id, text, retired) in workflows {
            let definition: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            entities.push(json!({"id":id,"kind":"workflow","name":definition["name"],"definition":definition,"retired":retired}));
        }
        for entity in &mut entities {
            let id = entity["id"].as_str().unwrap().to_string();
            let survivor: Option<String> = conn
                .query_row(
                    "SELECT survivor_id FROM consolidation_aliases WHERE duplicate_id=?1 AND (?2 IS NULL OR workforce_id=?2)",
                    params![&id,self.workforce_scope()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?;
            entity["consolidated_into"] = json!(survivor);
            let mut counts = json!({});
            for (label, hours) in [("1h", 1), ("6h", 6), ("1d", 24), ("7d", 168), ("30d", 720)] {
                let mut stmt=conn.prepare("SELECT kind,COUNT(*) FROM workforce_usage WHERE entity_id=?1 AND (?4 IS NULL OR workforce_id=?4) AND at_ms>?2 AND at_ms<=?3 GROUP BY kind").map_err(|e|e.to_string())?;
                let rows = stmt
                    .query_map(
                        params![id, now - hours * 3600000, now, self.workforce_scope()],
                        |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
                    )
                    .map_err(|e| e.to_string())?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| e.to_string())?;
                let mut window = json!({"assignment":0,"model_call":0});
                for (kind, count) in rows {
                    window[kind] = json!(count);
                }
                counts[label] = window;
            }
            entity["usage"] = counts;
            let last: Option<i64> = conn.query_row("SELECT MAX(at_ms) FROM workforce_usage WHERE entity_id=?1 AND kind='assignment' AND (?2 IS NULL OR workforce_id=?2)",params![&id,self.workforce_scope()],|r|r.get(0)).map_err(|e|e.to_string())?;
            entity["last_assignment_ms"] = json!(last);
            let last: Option<i64> = conn.query_row("SELECT MAX(at_ms) FROM workforce_usage WHERE entity_id=?1 AND kind='model_call' AND (?2 IS NULL OR workforce_id=?2)",params![&id,self.workforce_scope()],|r|r.get(0)).map_err(|e|e.to_string())?;
            entity["last_model_call_ms"] = json!(last);
        }
        let since: i64 = conn
            .query_row(
                "SELECT value FROM workforce_metadata WHERE key='tracking_since_ms'",
                [],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        drop(conn);
        let cursor = self.trace_cursor()?;
        Ok(json!({"entities":entities,"as_of_ms":now,"tracking_since_ms":since,"cursor":cursor}))
    }
}
