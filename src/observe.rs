//! Memory-first execution traces, immutable definition snapshots and controls.
//! SQL sequence orders the live view; disk retains an asynchronously published prefix.
use crate::portfolio::Portfolio;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
fn id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS trace_runs(
        id TEXT PRIMARY KEY, conversation_id TEXT, source TEXT NOT NULL, title TEXT NOT NULL,
        status TEXT NOT NULL, created_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS trace_requests(
        id TEXT PRIMARY KEY, run_id TEXT NOT NULL, root_span TEXT NOT NULL, current_span TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS trace_spans(
        id TEXT PRIMARY KEY, run_id TEXT NOT NULL, request_id TEXT NOT NULL, parent_id TEXT,
        kind TEXT NOT NULL, entity_id TEXT NOT NULL, status TEXT NOT NULL,
        started_ms INTEGER NOT NULL, ended_ms INTEGER, input TEXT NOT NULL,
        output TEXT, version_id TEXT, usage TEXT);
        CREATE TABLE IF NOT EXISTS trace_events(
        seq INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, span_id TEXT,
        kind TEXT NOT NULL, at_ms INTEGER NOT NULL, data TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS trace_events_run_seq ON trace_events(run_id,seq);
        CREATE INDEX IF NOT EXISTS trace_spans_run ON trace_spans(run_id);
        CREATE TABLE IF NOT EXISTS definition_versions(
        id TEXT PRIMARY KEY, entity_id TEXT NOT NULL, kind TEXT NOT NULL,
        definition TEXT NOT NULL, created_ms INTEGER NOT NULL,
        UNIQUE(entity_id,kind,definition));
        CREATE TABLE IF NOT EXISTS definition_changes(
        id INTEGER PRIMARY KEY, entity_id TEXT NOT NULL, kind TEXT NOT NULL,
        before_version TEXT, after_version TEXT, run_id TEXT, span_id TEXT,
        action TEXT NOT NULL, evidence TEXT NOT NULL, applied INTEGER NOT NULL,
        at_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS trace_tools(
        id TEXT PRIMARY KEY, run_id TEXT NOT NULL, span_id TEXT, status TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS trace_controls(
        id TEXT PRIMARY KEY, run_id TEXT NOT NULL, target TEXT NOT NULL,
        action TEXT NOT NULL, message TEXT NOT NULL, status TEXT NOT NULL,
        span_id TEXT, created_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS trace_gates(
        run_id TEXT NOT NULL, target TEXT NOT NULL, paused INTEGER NOT NULL DEFAULT 0,
        interrupt INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(run_id,target));
        CREATE TABLE IF NOT EXISTS conversations(
        id TEXT PRIMARY KEY, title TEXT NOT NULL, created_ms INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS conversation_messages(
        id INTEGER PRIMARY KEY, conversation_id TEXT NOT NULL, run_id TEXT,
        message TEXT NOT NULL, created_ms INTEGER NOT NULL);")?;
    // Preserve legacy immutable-definition history before model setup can rewrite
    // live metadata. The helper below detects whether workforce columns exist.
    archive_definitions(conn)?;
    // In-flight HTTP calls cannot be replayed safely after process loss.
    conn.execute("UPDATE trace_runs SET status='interrupted',updated_ms=?1 WHERE status IN ('running','paused')",[now_ms()])?;
    conn.execute("UPDATE trace_spans SET status='interrupted',ended_ms=?1 WHERE ended_ms IS NULL AND run_id IN (SELECT id FROM trace_runs WHERE status='interrupted')",[now_ms()])?;
    conn.execute("UPDATE trace_controls SET status='not_delivered' WHERE status='queued' AND run_id IN (SELECT id FROM trace_runs WHERE status='interrupted')",[])?;
    Ok(())
}

pub fn archive_definitions(conn: &Connection) -> rusqlite::Result<()> {
    let agents_scoped = has_column(conn, "agents", "workforce_id")?;
    let agents = {
        let sql = if agents_scoped {
            "SELECT id,name,purpose,system_prompt,model,workforce_id FROM agents"
        } else {
            "SELECT id,name,purpose,system_prompt,model FROM agents"
        };
        let mut stmt = conn.prepare(sql)?;
        stmt.query_map([], |r| {
            let workforce: Option<String> = if agents_scoped { r.get(5)? } else { None };
            Ok((json!({"id":r.get::<_,String>(0)?,"name":r.get::<_,String>(1)?,"purpose":r.get::<_,String>(2)?,"system_prompt":r.get::<_,String>(3)?,"model":r.get::<_,String>(4)?}), workforce))
        })?.collect::<Result<Vec<_>,_>>()?
    };
    for (agent, workforce) in agents {
        snapshot_conn(
            conn,
            agent["id"].as_str().unwrap(),
            "agent",
            &agent,
            workforce.as_deref(),
        )?;
    }
    let workflows = {
        let scoped = has_column(conn, "workflows", "workforce_id")?;
        let sql = if scoped {
            "SELECT id,definition,workforce_id FROM workflows"
        } else {
            "SELECT id,definition FROM workflows"
        };
        let mut stmt = conn.prepare(sql)?;
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                if scoped {
                    r.get::<_, Option<String>>(2)?
                } else {
                    None
                },
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    for (entity, text, workforce) in workflows {
        snapshot_conn(
            conn,
            &entity,
            "workflow",
            &serde_json::from_str(&text).unwrap_or(json!({"stored_definition":text})),
            workforce.as_deref(),
        )?;
    }
    // Earlier evaluations remain evidence even though no detailed trace existed.
    let assessments = {
        let scoped = has_column(conn, "evaluations", "workforce_id")?;
        let sql = if scoped {
            "SELECT entity_id,entity_kind,before_definition,after_definition,workforce_id FROM evaluations"
        } else {
            "SELECT entity_id,entity_kind,before_definition,after_definition FROM evaluations"
        };
        let mut stmt = conn.prepare(sql)?;
        stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, Option<String>>(3)?,
                if scoped {
                    r.get::<_, Option<String>>(4)?
                } else {
                    None
                },
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    for (entity, kind, before, after, workforce) in assessments {
        for text in std::iter::once(before).chain(after) {
            snapshot_conn(
                conn,
                &entity,
                &kind,
                &serde_json::from_str(&text).unwrap_or(json!({"stored_definition":text})),
                workforce.as_deref(),
            )?;
        }
    }
    Ok(())
}

fn has_column(conn: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    Ok(conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?
        .iter()
        .any(|name| name == column))
}

pub(crate) fn snapshot_conn(
    conn: &Connection,
    entity: &str,
    kind: &str,
    definition: &Value,
    workforce_id: Option<&str>,
) -> rusqlite::Result<String> {
    let mut normalized = redact(definition);
    if kind == "agent" && normalized.is_object() {
        normalized["id"] = json!(entity);
    }
    let value = normalized.to_string();
    let scoped = has_column(conn, "definition_versions", "workforce_id")?;
    let existing: Option<String> =
        if scoped {
            conn.query_row(
            "SELECT id FROM definition_versions WHERE entity_id=?1 AND kind=?2 AND definition=?3
             AND ((?4 IS NULL AND workforce_id IS NULL) OR workforce_id=?4)",
            params![entity, kind, value, workforce_id],
            |r| r.get(0),
        ).optional()?
        } else {
            conn.query_row(
            "SELECT id FROM definition_versions WHERE entity_id=?1 AND kind=?2 AND definition=?3",
            params![entity, kind, value],
            |r| r.get(0),
        ).optional()?
        };
    if let Some(id) = existing {
        return Ok(id);
    }
    let version = id();
    if scoped {
        conn.execute(
            "INSERT INTO definition_versions(id,entity_id,kind,definition,created_ms,workforce_id) VALUES(?1,?2,?3,?4,?5,?6)",
            params![version, entity, kind, value, now_ms(), workforce_id],
        )?;
    } else {
        conn.execute(
            "INSERT INTO definition_versions(id,entity_id,kind,definition,created_ms) VALUES(?1,?2,?3,?4,?5)",
            params![version, entity, kind, value, now_ms()],
        )?;
    }
    Ok(version)
}

pub fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let secret = matches!(
                        key.to_ascii_lowercase().as_str(),
                        "authorization"
                            | "api_key"
                            | "apikey"
                            | "password"
                            | "access_token"
                            | "refresh_token"
                    );
                    (
                        key.clone(),
                        if secret {
                            json!("[redacted]")
                        } else {
                            redact(value)
                        },
                    )
                })
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.iter().map(redact).collect()),
        _ => value.clone(),
    }
}

impl Portfolio {
    pub fn trace_emit(
        &self,
        run: &str,
        span: Option<&str>,
        kind: &str,
        data: &Value,
    ) -> Result<(), String> {
        crate::traffic::record(Some(run), span, data["request_id"].as_str(), kind, data);
        let conn = self.conn.lock().unwrap();
        let (workforce, owner) = if run.is_empty() {
            (
                self.workforce_scope().map(str::to_owned),
                self.key_scope().map(str::to_owned),
            )
        } else {
            let row: Option<(Option<String>, Option<String>)> = conn
                .query_row(
                    "SELECT workforce_id,owner_key_id FROM trace_runs WHERE id=?1",
                    [run],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
                .map_err(|error| error.to_string())?;
            let Some((workforce, owner)) = row else {
                return Err("unknown run".into());
            };
            if self
                .workforce_scope()
                .is_some_and(|scope| Some(scope) != workforce.as_deref())
                || self
                    .key_scope()
                    .is_some_and(|scope| Some(scope) != owner.as_deref())
            {
                return Err("unknown run".into());
            }
            (workforce, owner)
        };
        conn.execute(
            "INSERT INTO trace_events(run_id,span_id,kind,at_ms,data,workforce_id,owner_key_id) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![run, span, kind, now_ms(), redact(data).to_string(), workforce, owner],
        ).map_err(|e| e.to_string())?;
        drop(conn);
        let _ = self.trace_bus.send(());
        Ok(())
    }

    pub fn trace_begin(
        &self,
        request: &str,
        payload: &Value,
        source: &str,
    ) -> Result<String, String> {
        if let Some((run, _)) = self.trace_context(request)? {
            return Ok(run);
        }
        let conversation = payload
            .pointer("/metadata/ccm_conversation_id")
            .and_then(Value::as_str);
        let explicit = payload
            .pointer("/metadata/ccm_run_id")
            .and_then(Value::as_str);
        let tool_ids: Vec<_> = payload["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .rev()
            .take_while(|m| m["role"] == "tool")
            .filter_map(|m| m["tool_call_id"].as_str())
            .collect();
        if tool_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != tool_ids.len()
        {
            return Err("duplicate tool results".into());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let mut resumed: Option<String> = None;
        for tool in &tool_ids {
            let owner: Option<String> = tx
                .query_row(
                    "SELECT t.run_id FROM trace_tools t JOIN trace_runs r ON r.id=t.run_id
                     WHERE t.id=?1 AND t.status='waiting'
                     AND (?2 IS NULL OR r.workforce_id=?2) AND (?3 IS NULL OR r.owner_key_id=?3)",
                    params![tool, self.workforce_scope(), self.key_scope()],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| e.to_string())?;
            if let Some(owner) = owner {
                if resumed.as_ref().is_some_and(|r| r != &owner) {
                    return Err("tool results belong to different runs".into());
                }
                resumed = Some(owner);
            } else {
                return Err("tool result has no active trace owner; it may have been reset or already consumed".into());
            }
        }
        if explicit.is_some() && resumed.as_deref().is_some_and(|r| Some(r) != explicit) {
            return Err("run ID conflicts with tool owner".into());
        }
        let existing = resumed.as_deref().or(explicit);
        let run = existing.map(str::to_string).unwrap_or_else(id);
        let root = id()[..16].to_string();
        let mut continuation_parent: Option<String> = None;
        let mut completed_tools = Vec::new();
        if let Some(conversation) = conversation {
            let title = payload["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .rev()
                .find(|message| message["role"] == "user")
                .and_then(|message| message["content"].as_str())
                .unwrap_or("Conversation")
                .chars()
                .take(100)
                .collect::<String>();
            tx.execute(
                "INSERT OR IGNORE INTO conversations(id,title,created_ms,workforce_id,owner_key_id)
                 VALUES(?1,?2,?3,?4,?5)",
                params![
                    conversation,
                    title,
                    now_ms(),
                    self.workforce_scope(),
                    self.key_scope()
                ],
            )
            .map_err(|e| e.to_string())?;
            if self.workforce_scope().is_some() || self.key_scope().is_some() {
                let owned: bool = tx
                    .query_row(
                        "SELECT EXISTS(SELECT 1 FROM conversations WHERE id=?1
                     AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",
                        params![conversation, self.workforce_scope(), self.key_scope()],
                        |row| row.get(0),
                    )
                    .map_err(|e| e.to_string())?;
                if !owned {
                    return Err("conversation belongs to another key or workforce".into());
                }
            }
        }
        if existing.is_some() {
            let owned: Option<(String, Option<String>, Option<String>)> = tx
                .query_row("SELECT status FROM trace_runs WHERE id=?1", [&run], |r| {
                    Ok((r.get(0)?, None, None))
                })
                .optional()
                .map_err(|e| e.to_string())?;
            // Check ownership independently of the supplied tool identifiers: an
            // explicit run ID alone is never sufficient to resume another key's run.
            let owned = if self.workforce_scope().is_some() || self.key_scope().is_some() {
                tx.query_row(
                    "SELECT status,workforce_id,owner_key_id FROM trace_runs WHERE id=?1
                     AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                    params![&run, self.workforce_scope(), self.key_scope()],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )
                .optional()
                .map_err(|e| e.to_string())?
            } else {
                owned
            };
            let status = owned.map(|(status, _, _)| status);
            if status.as_deref() != Some("waiting_tools") {
                return Err("run is not awaiting a tool continuation".into());
            }
            if tool_ids.is_empty() {
                return Err("continuation requires tool results".into());
            }
            let waiting: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM trace_tools WHERE run_id=?1 AND status='waiting'",
                    [&run],
                    |r| r.get(0),
                )
                .map_err(|e| e.to_string())?;
            if waiting != tool_ids.len() as i64 {
                return Err("continuation requires every outstanding tool result".into());
            }
            tx.execute(
                "UPDATE trace_runs SET status='running',updated_ms=?2 WHERE id=?1
                 AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4)",
                params![run, now_ms(), self.workforce_scope(), self.key_scope()],
            )
            .map_err(|e| e.to_string())?;
            for tool in &tool_ids {
                let span: Option<String> = tx
                    .query_row("SELECT span_id FROM trace_tools WHERE id=?1", [tool], |r| {
                        r.get(0)
                    })
                    .optional()
                    .map_err(|e| e.to_string())?
                    .flatten();
                if continuation_parent.is_none() {
                    continuation_parent = span.clone();
                }
                if let Some(span) = &span {
                    completed_tools.push(span.clone());
                }
                let output = payload["messages"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .rev()
                    .find(|m| m["tool_call_id"].as_str() == Some(*tool));
                tx.execute(
                    "UPDATE trace_tools SET status='returned' WHERE id=?1",
                    [tool],
                )
                .map_err(|e| e.to_string())?;
                tx.execute(
                    "UPDATE trace_spans SET status='completed',ended_ms=?2,output=?3 WHERE id=?1",
                    params![span, now_ms(), output.map(|v| redact(v).to_string())],
                )
                .map_err(|e| e.to_string())?;
            }
        } else {
            if source == "web" && conversation.is_some() {
                let active:i64=tx.query_row("SELECT COUNT(*) FROM trace_runs WHERE conversation_id=?1 AND status IN ('running','paused','waiting_tools') AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",params![conversation,self.workforce_scope(),self.key_scope()],|r|r.get(0)).map_err(|e|e.to_string())?;
                if active > 0 {
                    return Err(
                        "conversation is active; send guidance or interrupt the active run".into(),
                    );
                }
            }
            let title = payload["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .rev()
                .find(|m| m["role"] == "user")
                .and_then(|m| m["content"].as_str())
                .unwrap_or("Execution")
                .chars()
                .take(100)
                .collect::<String>();
            tx.execute(
                "INSERT INTO trace_runs(id,conversation_id,source,title,status,created_ms,updated_ms,workforce_id,owner_key_id)
                 VALUES(?1,?2,?3,?4,'running',?5,?5,?6,?7)",
                params![run, conversation, source, title, now_ms(),self.workforce_scope(),self.key_scope()],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.execute(
            "INSERT INTO trace_requests VALUES(?1,?2,?3,?3)",
            params![request, run, root],
        )
        .map_err(|e| e.to_string())?;
        tx.execute("INSERT INTO trace_spans(id,run_id,request_id,parent_id,kind,entity_id,status,started_ms,input,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,'request','foreman','running',?5,?6,?7,?8)",params![root,run,request,continuation_parent,now_ms(),redact(payload).to_string(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);
        self.trace_emit(
            &run,
            Some(&root),
            "request.started",
            &json!({"request_id":request,"source":source,"input":payload}),
        )?;
        if !tool_ids.is_empty() {
            self.trace_emit(
                &run,
                Some(&root),
                "tools.returned",
                &json!({"ids":tool_ids}),
            )?;
        }
        for span in completed_tools {
            self.export_trace(&run, Some(&span));
        }
        Ok(run)
    }

    pub fn trace_context(&self, request: &str) -> Result<Option<(String, String)>, String> {
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT q.run_id,q.current_span FROM trace_requests q JOIN trace_runs r ON r.id=q.run_id
                 WHERE q.id=?1 AND (?2 IS NULL OR r.workforce_id=?2) AND (?3 IS NULL OR r.owner_key_id=?3)",
                params![request,self.workforce_scope(),self.key_scope()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(|e| e.to_string())
    }

    pub fn trace_conversation(&self, run: &str) -> Result<Option<String>, String> {
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT conversation_id FROM trace_runs WHERE id=?1
                 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
                params![run, self.workforce_scope(), self.key_scope()],
                |row| row.get(0),
            )
            .optional()
            .map(|conversation| conversation.flatten())
            .map_err(|error| error.to_string())
    }

    pub fn trace_event(&self, request: &str, kind: &str, data: &Value) -> Result<(), String> {
        if let Some((run, span)) = self.trace_context(request)? {
            self.trace_emit(&run, Some(&span), kind, data)?;
        }
        Ok(())
    }

    pub fn trace_finish_request(
        &self,
        request: &str,
        status: &str,
        output: &Value,
    ) -> Result<(), String> {
        let conn = self.conn.lock().unwrap();
        let context: Option<(String, String)> = conn
            .query_row(
                "SELECT q.run_id,q.root_span FROM trace_requests q JOIN trace_runs r ON r.id=q.run_id
                 WHERE q.id=?1 AND (?2 IS NULL OR r.workforce_id=?2) AND (?3 IS NULL OR r.owner_key_id=?3)",
                params![request,self.workforce_scope(),self.key_scope()],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        let Some((run, span)) = context else {
            return Ok(());
        };
        conn.execute(
            "UPDATE trace_runs SET status=?2,updated_ms=?3 WHERE id=?1 AND status!='reset'",
            params![run, status, now_ms()],
        )
        .map_err(|e| e.to_string())?;
        conn.execute("UPDATE trace_spans SET status=?2,ended_ms=?3,output=?4 WHERE id=?1 AND ended_ms IS NULL",params![span,status,now_ms(),redact(output).to_string()]).map_err(|e|e.to_string())?;
        if status != "waiting_tools" {
            conn.execute("UPDATE trace_controls SET status='not_delivered' WHERE run_id=?1 AND status='queued'",[&run]).map_err(|e|e.to_string())?;
        }
        drop(conn);
        let actual: String = self
            .conn
            .lock()
            .unwrap()
            .query_row("SELECT status FROM trace_runs WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)", params![&run,self.workforce_scope(),self.key_scope()], |r| {
                r.get(0)
            })
            .map_err(|e| e.to_string())?;
        self.trace_emit(
            &run,
            Some(&span),
            "request.finished",
            &json!({"status":actual,"output":output}),
        )?;
        self.export_trace(&run, Some(&span));
        Ok(())
    }

    pub fn trace_bind_tools(&self, request: &str, message: &Value) -> Result<(), String> {
        if let Some((run, parent)) = self.trace_context(request)? {
            let mut conn = self.conn.lock().unwrap();
            let tx = conn.transaction().map_err(|e| e.to_string())?;
            let changed=tx.execute("UPDATE trace_runs SET status='waiting_tools',updated_ms=?2 WHERE id=?1 AND status='running'",params![run,now_ms()]).map_err(|e|e.to_string())?;
            if changed != 1 {
                return Err("run cannot issue tools after completion or reset".into());
            }
            for call in message["tool_calls"].as_array().into_iter().flatten() {
                let span = id()[..16].to_string();
                tx.execute("INSERT INTO trace_spans(id,run_id,request_id,parent_id,kind,entity_id,status,started_ms,input,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,'client_tool',?5,'waiting_tools',?6,?7,?8,?9)",params![span,run,request,parent,call.pointer("/function/name").and_then(Value::as_str).unwrap_or("tool"),now_ms(),redact(call).to_string(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
                tx.execute(
                    "INSERT INTO trace_tools(id,run_id,span_id,status,workforce_id,owner_key_id) VALUES(?1,?2,?3,'waiting',?4,?5)",
                    params![call["id"].as_str(), run, span,self.workforce_scope(),self.key_scope()],
                )
                .map_err(|e| e.to_string())?;
                tx.execute("INSERT INTO trace_events(run_id,span_id,kind,at_ms,data,workforce_id,owner_key_id) VALUES(?1,?2,'tool.waiting',?3,?4,?5,?6)",params![run,span,now_ms(),redact(call).to_string(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
            }
            tx.commit().map_err(|e| e.to_string())?;
            drop(conn);
            let _ = self.trace_bus.send(());
        }
        Ok(())
    }

    pub fn trace_snapshot(
        &self,
        entity: &str,
        kind: &str,
        definition: &Value,
    ) -> Result<String, String> {
        snapshot_conn(
            &self.conn.lock().unwrap(),
            entity,
            kind,
            definition,
            self.workforce_scope(),
        )
        .map_err(|e| e.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn trace_changes(
        &self,
        request: &str,
        entity: &str,
        kind: &str,
        before: Option<&Value>,
        after: Option<&Value>,
        action: &str,
        evidence: &Value,
        applied: bool,
    ) -> Result<(), String> {
        let previous = before
            .map(|v| self.trace_snapshot(entity, kind, v))
            .transpose()?;
        let next = after
            .map(|v| self.trace_snapshot(entity, kind, v))
            .transpose()?;
        let context = self.trace_context(request)?;
        self.conn.lock().unwrap().execute("INSERT INTO definition_changes(entity_id,kind,before_version,after_version,run_id,span_id,action,evidence,applied,at_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",params![entity,kind,previous,next,context.as_ref().map(|c|&c.0),context.as_ref().map(|c|&c.1),action,redact(evidence).to_string(),applied,now_ms(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
        self.trace_event(request,"definition.changed",&json!({"entity_id":entity,"entity_kind":kind,"before_version":previous,"after_version":next,"action":action,"evidence":evidence,"applied":applied}))
    }

    pub fn trace_events(&self, after: i64, run: Option<&str>) -> Result<Vec<Value>, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt=conn.prepare("SELECT seq,run_id,span_id,kind,at_ms,data FROM trace_events WHERE seq>?1 AND (?2 IS NULL OR run_id=?2) AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id IS NULL OR owner_key_id=?4) ORDER BY seq LIMIT 500").map_err(|e|e.to_string())?;
        stmt.query_map(params![after,run,self.workforce_scope(),self.key_scope()],|r|Ok(json!({"seq":r.get::<_,i64>(0)?,"run_id":r.get::<_,String>(1)?,"span_id":r.get::<_,Option<String>>(2)?,"kind":r.get::<_,String>(3)?,"at_ms":r.get::<_,i64>(4)?,"data":serde_json::from_str::<Value>(&r.get::<_,String>(5)?).unwrap_or(Value::Null)}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())
    }

    pub fn trace_cursor(&self) -> Result<i64, String> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT COALESCE(MAX(seq),0) FROM trace_events WHERE (?1 IS NULL OR workforce_id=?1)
             AND (?2 IS NULL OR owner_key_id IS NULL OR owner_key_id=?2)",
            params![self.workforce_scope(), self.key_scope()],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())
    }

    #[cfg(test)]
    pub fn trace_runs(&self) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt=conn.prepare("SELECT id,conversation_id,source,title,status,created_ms,updated_ms FROM trace_runs ORDER BY created_ms DESC LIMIT 200").map_err(|e|e.to_string())?;
        let rows=stmt.query_map([],|r|Ok(json!({"id":r.get::<_,String>(0)?,"conversation_id":r.get::<_,Option<String>>(1)?,"source":r.get::<_,String>(2)?,"title":r.get::<_,String>(3)?,"status":r.get::<_,String>(4)?,"created_ms":r.get::<_,i64>(5)?,"updated_ms":r.get::<_,i64>(6)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        Ok(json!(rows))
    }

    pub fn trace_detail(&self, run: &str) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let accessible: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM trace_runs WHERE id=?1
             AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3))",
                params![run, self.workforce_scope(), self.key_scope()],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if !accessible {
            return Err("unknown run".into());
        }
        let mut stmt=conn.prepare("SELECT id,parent_id,kind,entity_id,status,started_ms,ended_ms,input,output,version_id,usage,request_id FROM trace_spans WHERE run_id=?1 ORDER BY started_ms,rowid").map_err(|e|e.to_string())?;
        let mut spans=stmt.query_map([run],|r|Ok(json!({"id":r.get::<_,String>(0)?,"parent_id":r.get::<_,Option<String>>(1)?,"kind":r.get::<_,String>(2)?,"entity_id":r.get::<_,String>(3)?,"status":r.get::<_,String>(4)?,"started_ms":r.get::<_,i64>(5)?,"ended_ms":r.get::<_,Option<i64>>(6)?,"input":parse(r.get::<_,Option<String>>(7)?),"output":parse(r.get::<_,Option<String>>(8)?),"version_id":r.get::<_,Option<String>>(9)?,"usage":parse(r.get::<_,Option<String>>(10)?),"request_id":r.get::<_,String>(11)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        drop(stmt);
        for span in spans
            .iter_mut()
            .filter(|s| s["output"].is_null() && s["status"] == "running")
        {
            let mut stmt=conn.prepare("SELECT data FROM trace_events WHERE span_id=?1 AND kind='model.delta' ORDER BY seq").map_err(|e|e.to_string())?;
            let chunks = stmt
                .query_map([span["id"].as_str().unwrap()], |r| r.get::<_, String>(0))
                .map_err(|e| e.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.to_string())?;
            drop(stmt);
            if !chunks.is_empty() {
                let mut collector = crate::upstream::Collector::default();
                for chunk in chunks {
                    if let Ok(value) = serde_json::from_str(&chunk) {
                        let _ = collector.feed(&value);
                    }
                }
                span["output"] = collector.finish();
                span["partial"] = json!(true);
            }
        }
        let mut stmt=conn.prepare("SELECT id,target,action,message,status,span_id,created_ms FROM trace_controls WHERE run_id=?1 ORDER BY created_ms,rowid").map_err(|e|e.to_string())?;
        let controls=stmt.query_map([run],|r|Ok(json!({"id":r.get::<_,String>(0)?,"target":r.get::<_,String>(1)?,"action":r.get::<_,String>(2)?,"message":r.get::<_,String>(3)?,"status":r.get::<_,String>(4)?,"span_id":r.get::<_,Option<String>>(5)?,"created_ms":r.get::<_,i64>(6)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        drop(stmt);
        let mut stmt=conn.prepare("SELECT id,entity_id,kind,definition,created_ms FROM definition_versions WHERE (?2 IS NULL OR workforce_id=?2) AND (id IN(SELECT version_id FROM trace_spans WHERE run_id=?1) OR id IN(SELECT before_version FROM definition_changes WHERE run_id=?1) OR id IN(SELECT after_version FROM definition_changes WHERE run_id=?1)) ORDER BY created_ms").map_err(|e|e.to_string())?;
        let versions=stmt.query_map(params![run,self.workforce_scope()],|r|Ok(json!({"id":r.get::<_,String>(0)?,"entity_id":r.get::<_,String>(1)?,"kind":r.get::<_,String>(2)?,"definition":parse(r.get::<_,Option<String>>(3)?),"created_ms":r.get::<_,i64>(4)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        drop(stmt);
        let mut stmt=conn.prepare("SELECT entity_id,kind,before_version,after_version,action,evidence,applied,at_ms FROM definition_changes WHERE run_id=?1 AND (?2 IS NULL OR owner_key_id=?2) ORDER BY id").map_err(|e|e.to_string())?;
        let changes=stmt.query_map(params![run,self.key_scope()],|r|Ok(json!({"entity_id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"before_version":r.get::<_,Option<String>>(2)?,"after_version":r.get::<_,Option<String>>(3)?,"action":r.get::<_,String>(4)?,"evidence":parse(r.get::<_,Option<String>>(5)?),"applied":r.get::<_,bool>(6)?,"at_ms":r.get::<_,i64>(7)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        drop(stmt);
        let mut stmt=conn.prepare("SELECT seq,span_id,kind,at_ms,data FROM (SELECT seq,span_id,kind,at_ms,data FROM trace_events WHERE run_id=?1 AND kind NOT IN ('model.delta','span.input') ORDER BY seq DESC LIMIT 200) ORDER BY seq").map_err(|e|e.to_string())?;
        let events=stmt.query_map([run],|r|Ok(json!({"seq":r.get::<_,i64>(0)?,"run_id":run,"span_id":r.get::<_,Option<String>>(1)?,"kind":r.get::<_,String>(2)?,"at_ms":r.get::<_,i64>(3)?,"data":parse(r.get::<_,Option<String>>(4)?)}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        drop(stmt);
        drop(conn);
        let cursor = self.trace_cursor()?;
        let conn = self.conn.lock().unwrap();
        let improvements =
            crate::improvement::list_scoped(&conn, run, self.workforce_scope(), self.key_scope())?;
        let presentations = crate::transparency::snapshot(&conn, run)?;
        let creation_policy: Option<String> = conn.query_row("SELECT context FROM request_creation_policy WHERE run_id=?1 ORDER BY rowid DESC LIMIT 1",[run],|r|r.get(0)).optional().map_err(|e|e.to_string())?;
        let creation_policy = creation_policy
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .map(|v| json!({"policy":v["policy"],"phase":v["phase"],"orders":v["orders"]}));
        Ok(
            json!({"run_id":run,"spans":spans,"controls":controls,"versions":versions,"changes":changes,"events":events,"cursor":cursor,"improvements":improvements,"creation_policy":creation_policy,"presentations":presentations}),
        )
    }

    pub fn control(
        &self,
        run: &str,
        target: &str,
        action: &str,
        message: &str,
    ) -> Result<Value, String> {
        if !["guidance", "interrupt", "pause", "resume"].contains(&action) {
            return Err("unknown control action".into());
        }
        if ["guidance", "interrupt"].contains(&action) && message.trim().is_empty() {
            return Err("guidance text is required".into());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let status: String = tx
            .query_row("SELECT status FROM trace_runs WHERE id=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)", params![run,self.workforce_scope(),self.key_scope()], |r| {
                r.get(0)
            })
            .map_err(|_| "unknown run")?;
        if !["running", "paused", "waiting_tools"].contains(&status.as_str()) {
            return Err("run is no longer active".into());
        }
        if target != "*" {
            let count:i64=tx.query_row("SELECT COUNT(*) FROM trace_spans WHERE run_id=?1 AND entity_id=?2 AND kind!='client_tool'",params![run,target],|r|r.get(0)).map_err(|e|e.to_string())?;
            if count == 0 {
                return Err("unknown agent in this run".into());
            }
        }
        let control_id = id();
        let status = if ["pause", "resume"].contains(&action) {
            "accepted"
        } else {
            "queued"
        };
        tx.execute(
            "INSERT INTO trace_controls(id,run_id,target,action,message,status,span_id,created_ms,workforce_id,owner_key_id)
             VALUES(?1,?2,?3,?4,?5,?6,NULL,?7,?8,?9)",
            params![control_id, run, target, action, message, status, now_ms(),self.workforce_scope(),self.key_scope()],
        )
        .map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT OR IGNORE INTO trace_gates(run_id,target) VALUES(?1,?2)",
            params![run, target],
        )
        .map_err(|e| e.to_string())?;
        if action == "pause" || action == "resume" {
            tx.execute(
                "UPDATE trace_gates SET paused=?3 WHERE run_id=?1 AND target=?2",
                params![run, target, action == "pause"],
            )
            .map_err(|e| e.to_string())?;
        }
        if action == "interrupt" {
            tx.execute("UPDATE trace_gates SET interrupt=interrupt+1,paused=0 WHERE run_id=?1 AND target=?2",params![run,target]).map_err(|e|e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);
        let value = json!({"id":control_id,"target":target,"action":action,"message":message,"status":status});
        self.trace_emit(run, None, "control.accepted", &value)?;
        Ok(value)
    }

    pub fn gate(&self, run: &str, entity: &str) -> Result<(bool, i64), String> {
        let conn = self.conn.lock().unwrap();
        let status: String = conn
            .query_row("SELECT status FROM trace_runs WHERE id=?1", [run], |r| {
                r.get(0)
            })
            .map_err(|e| e.to_string())?;
        if status == "reset" {
            return Err("run cancelled by reset".into());
        }
        conn.query_row("SELECT COALESCE(MAX(paused),0),COALESCE(SUM(interrupt),0) FROM trace_gates WHERE run_id=?1 AND (target='*' OR target=?2)",params![run,entity],|r|Ok((r.get(0)?,r.get(1)?))).map_err(|e|e.to_string())
    }

    pub async fn boundary(&self, run: &str, entity: &str) -> Result<(), String> {
        let mut rx = self.trace_bus.subscribe();
        let mut announced = false;
        while self.gate(run, entity)?.0 {
            if !announced {
                self.conn.lock().unwrap().execute("UPDATE trace_runs SET status='paused',updated_ms=?2 WHERE id=?1 AND status='running'",params![run,now_ms()]).map_err(|e|e.to_string())?;
                self.trace_emit(run, None, "execution.paused", &json!({"entity_id":entity}))?;
                announced = true;
            }
            let _ = rx.recv().await;
        }
        if announced {
            self.conn.lock().unwrap().execute("UPDATE trace_runs SET status='running',updated_ms=?2 WHERE id=?1 AND status='paused'",params![run,now_ms()]).map_err(|e|e.to_string())?;
            self.trace_emit(run, None, "execution.resumed", &json!({"entity_id":entity}))?;
        }
        Ok(())
    }

    pub fn take_guidance(&self, run: &str, entity: &str, span: &str) -> Result<Vec<Value>, String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        let controls = {
            let mut stmt=tx.prepare("SELECT id,message FROM trace_controls WHERE run_id=?1 AND (target='*' OR target=?2) AND status='queued' ORDER BY created_ms,rowid").map_err(|e|e.to_string())?;
            stmt.query_map(params![run, entity], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })
            .map_err(|e| e.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?
        };
        for (id, _) in &controls {
            tx.execute(
                "UPDATE trace_controls SET status='delivered',span_id=?2 WHERE id=?1",
                params![id, span],
            )
            .map_err(|e| e.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);
        for (id, _) in &controls {
            self.trace_emit(
                run,
                Some(span),
                "control.delivered",
                &json!({"id":id,"target":entity}),
            )?;
        }
        Ok(controls.into_iter().map(|(_,text)|json!({"role":"user","content":format!("Operator guidance for this execution:\n{text}")})).collect())
    }
}

fn parse(text: Option<String>) -> Value {
    text.and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(Value::Null)
}

pub struct Span {
    pub run: String,
    pub id: String,
    kind: String,
    entity: String,
    started: std::time::Instant,
    portfolio: Arc<Portfolio>,
    request: String,
    parent: String,
    group: bool,
    finished: bool,
}

impl Span {
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        portfolio: Arc<Portfolio>,
        request: &str,
        kind: &str,
        entity: &str,
        input: &Value,
        version: Option<&str>,
        group: bool,
    ) -> Result<Self, String> {
        portfolio.trace_begin(request, &json!({"messages":[]}), "internal")?;
        let (run, parent) = portfolio
            .trace_context(request)?
            .ok_or("missing trace context")?;
        let span = id()[..16].to_string();
        {
            let conn = portfolio.conn.lock().unwrap();
            conn.execute("INSERT INTO trace_spans(id,run_id,request_id,parent_id,kind,entity_id,status,started_ms,input,version_id,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6,'running',?7,?8,?9,?10,?11)",params![span,run,request,parent,kind,entity,now_ms(),redact(input).to_string(),version,portfolio.workforce_scope(),portfolio.key_scope()]).map_err(|e|e.to_string())?;
            if group {
                conn.execute(
                    "UPDATE trace_requests SET current_span=?2 WHERE id=?1",
                    params![request, span],
                )
                .map_err(|e| e.to_string())?;
            }
        }
        portfolio.trace_emit(&run,Some(&span),"span.started",&json!({"kind":kind,"entity_id":entity,"parent_id":parent,"input":input,"version_id":version}))?;
        Ok(Self {
            run,
            id: span,
            kind: kind.into(),
            entity: entity.into(),
            started: std::time::Instant::now(),
            portfolio,
            request: request.into(),
            parent,
            group,
            finished: false,
        })
    }

    pub fn input(&self, input: &Value) -> Result<(), String> {
        self.portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE trace_spans SET input=?2 WHERE id=?1",
                params![self.id, redact(input).to_string()],
            )
            .map_err(|e| e.to_string())?;
        self.portfolio
            .trace_emit(&self.run, Some(&self.id), "span.input", input)
    }

    pub fn emit(&self, kind: &str, data: &Value) -> Result<(), String> {
        self.portfolio
            .trace_emit(&self.run, Some(&self.id), kind, data)
    }

    pub fn finish(
        &mut self,
        status: &str,
        output: &Value,
        usage: Option<&Value>,
    ) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        let actual_status = {
            let conn = self.portfolio.conn.lock().unwrap();
            conn.execute("UPDATE trace_spans SET status=?2,ended_ms=?3,output=?4,usage=?5 WHERE id=?1 AND status!='reset'",params![self.id,status,now_ms(),redact(output).to_string(),usage.map(Value::to_string)]).map_err(|e|e.to_string())?;
            if self.group {
                conn.execute(
                    "UPDATE trace_requests SET current_span=?2 WHERE id=?1 AND current_span=?3",
                    params![self.request, self.parent, self.id],
                )
                .map_err(|e| e.to_string())?;
            }
            conn.query_row(
                "SELECT status FROM trace_spans WHERE id=?1",
                [&self.id],
                |r| r.get::<_, String>(0),
            )
            .map_err(|e| e.to_string())?
        };
        self.finished = true;
        self.emit(
            "span.finished",
            &json!({"status":actual_status,"output":output,"usage":usage}),
        )?;
        self.portfolio.export_trace(&self.run, Some(&self.id));
        Ok(())
    }
}

impl Portfolio {
    fn export_trace(&self, run: &str, only: Option<&str>) {
        let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") else {
            return;
        };
        let Ok(payload) = self.otlp(run, only) else {
            return;
        };
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                match reqwest::Client::new()
                    .post(endpoint)
                    .timeout(std::time::Duration::from_secs(5))
                    .json(&payload)
                    .send()
                    .await
                {
                    Ok(response) if response.status().is_success() => {}
                    other => tracing::warn!(result=?other,"OTLP trace export failed"),
                }
            });
        }
    }
}

impl Drop for Span {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.portfolio.event(
                &self.request,
                &self.kind,
                &self.entity,
                "cancelled",
                self.started.elapsed().as_millis() as u64,
                None,
                "operation dropped before completion",
            );
            let _ = self.finish(
                "cancelled",
                &json!({"reason":"operation dropped before completion"}),
                None,
            );
        }
    }
}

impl Portfolio {
    /// OTLP/HTTP JSON, usable by collectors without a mandatory external service.
    pub fn otlp(&self, run: &str, only: Option<&str>) -> Result<Value, String> {
        let detail = self.trace_detail(run)?;
        let spans:Vec<_>=detail["spans"].as_array().unwrap().iter().filter(|s|only.is_none_or(|id|s["id"]==id)).filter(|s|!s["ended_ms"].is_null()).map(|s| {
            let mut value=json!({"traceId":run,"spanId":s["id"],"name":format!("{} {}",s["kind"].as_str().unwrap(),s["entity_id"].as_str().unwrap()),"kind":1,
                "startTimeUnixNano":(s["started_ms"].as_i64().unwrap() as u64*1_000_000).to_string(),
                "endTimeUnixNano":(s["ended_ms"].as_i64().unwrap() as u64*1_000_000).to_string(),
                "attributes":[{"key":"ccm.entity.id","value":{"stringValue":s["entity_id"]}},{"key":"ccm.status","value":{"stringValue":s["status"]}},{"key":"ccm.request.id","value":{"stringValue":s["request_id"]}},{"key":"ccm.definition.version","value":{"stringValue":s["version_id"].as_str().unwrap_or("")}}],
                "status":{"code":if s["status"]=="error"{2}else{1}}});
            if !s["parent_id"].is_null(){value["parentSpanId"]=s["parent_id"].clone();}value
        }).collect();
        Ok(
            json!({"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"ccm"}}]},"scopeSpans":[{"scope":{"name":"ccm","version":env!("CARGO_PKG_VERSION")},"spans":spans}]}]}),
        )
    }
}
