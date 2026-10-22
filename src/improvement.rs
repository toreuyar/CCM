//! Durable automatic improvement, released only after the final response body.
use crate::{observe::now_ms, portfolio::Portfolio, state::AppState};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::sync::Arc;

const DEFAULT_TURN_INTERVAL: u32 = 10;
const MIN_TURN_INTERVAL: u32 = 5;
const MAX_TURN_INTERVAL: u32 = 30;
const DEFAULT_ELAPSED_INTERVAL_MS: u64 = 60 * 60 * 1000;
const SHARED_SUGGESTION_NOTE: &str = "based on private checkpoint evidence; details withheld";
pub(crate) const AUTO_PRIVATE_AUTHORIZATION_CHANGED: &str =
    "automatic private-workforce application is no longer authorized";
pub(crate) const MAX_SCREENING_EVIDENCE_BYTES: usize = 12 * 1024;
pub(crate) const MAX_EVALUATION_EVIDENCE_BYTES: usize = 32 * 1024;
const MAX_CAPTURED_PROMPT_BYTES: usize = 8 * 1024;
const MAX_PROMPT_SAMPLES: i64 = 64;
const MAX_RUN_SAMPLES: i64 = 16;
const MAX_SPAN_SAMPLES: i64 = 64;
const MAX_EVENT_SAMPLES: i64 = 64;
const MAX_TRACE_FIELD_CHARS: usize = 1024;

/// Authority for applying a generated suggestion. Explicit user/API applies keep
/// the existing shared-workforce declassification rules. Background application
/// is a separate capability that is valid only for the contributing key's current
/// single-key private workforce and is rechecked in the definition mutation tx.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ApplicationAuthority {
    Explicit,
    AutoPrivate {
        source_key_id: String,
        workforce_id: String,
        job_id: String,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ImprovementConfig {
    pub turns: TurnCheckpointConfig,
    pub elapsed: ElapsedCheckpointConfig,
    pub foreground_tokens: TokenCheckpointConfig,
}

impl Default for ImprovementConfig {
    fn default() -> Self {
        Self {
            turns: TurnCheckpointConfig::default(),
            elapsed: ElapsedCheckpointConfig::default(),
            foreground_tokens: TokenCheckpointConfig::default(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TurnCheckpointConfig {
    pub enabled: bool,
    pub initial_interval: u32,
    pub minimum_interval: u32,
    pub maximum_interval: u32,
}

impl Default for TurnCheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            initial_interval: DEFAULT_TURN_INTERVAL,
            minimum_interval: MIN_TURN_INTERVAL,
            maximum_interval: MAX_TURN_INTERVAL,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ElapsedCheckpointConfig {
    pub enabled: bool,
    pub interval_ms: u64,
}

impl Default for ElapsedCheckpointConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_ms: DEFAULT_ELAPSED_INTERVAL_MS,
        }
    }
}

#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct TokenCheckpointConfig {
    pub enabled: bool,
    /// A missing threshold keeps token checkpoints disabled even if enabled=true.
    pub threshold: Option<u64>,
}

impl ImprovementConfig {
    pub fn validate(&self) -> Result<(), String> {
        if !(MIN_TURN_INTERVAL..=MAX_TURN_INTERVAL).contains(&self.turns.minimum_interval)
            || !(MIN_TURN_INTERVAL..=MAX_TURN_INTERVAL).contains(&self.turns.maximum_interval)
            || self.turns.minimum_interval > self.turns.maximum_interval
            || !(self.turns.minimum_interval..=self.turns.maximum_interval)
                .contains(&self.turns.initial_interval)
        {
            return Err("improvement.turns intervals must be ordered within 5..=30".into());
        }
        if self.elapsed.interval_ms == 0 || self.elapsed.interval_ms > 365 * 24 * 60 * 60 * 1000 {
            return Err("improvement.elapsed.interval_ms must be within 1..=31536000000".into());
        }
        if self.foreground_tokens.enabled
            && self
                .foreground_tokens
                .threshold
                .is_some_and(|threshold| threshold == 0)
        {
            return Err("improvement.foreground_tokens.threshold must be positive".into());
        }
        Ok(())
    }
}

pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch("CREATE TABLE IF NOT EXISTS improvement_epoch(id INTEGER PRIMARY KEY CHECK(id=1),value INTEGER NOT NULL);
        INSERT OR IGNORE INTO improvement_epoch VALUES(1,0);
        CREATE TABLE IF NOT EXISTS improvement_run_epochs(run_id TEXT PRIMARY KEY,epoch INTEGER NOT NULL);
        INSERT OR IGNORE INTO improvement_run_epochs(run_id,epoch) SELECT id,-1 FROM trace_runs;
        CREATE TRIGGER IF NOT EXISTS improvement_run_epoch_created AFTER INSERT ON trace_runs BEGIN
          INSERT OR IGNORE INTO improvement_run_epochs(run_id,epoch)
          SELECT NEW.id,value FROM improvement_epoch WHERE id=1;
        END;
        CREATE TABLE IF NOT EXISTS improvement_responses(request_id TEXT PRIMARY KEY,status TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS improvement_jobs(
        id TEXT PRIMARY KEY, origin_request TEXT NOT NULL, origin_run TEXT NOT NULL,
        entity_kind TEXT NOT NULL, entity_id TEXT NOT NULL, definition TEXT NOT NULL,
        task TEXT NOT NULL, result TEXT NOT NULL, epoch INTEGER NOT NULL,
        status TEXT NOT NULL, created_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
        evaluation_request TEXT, evaluation_run TEXT, outcome TEXT, attempts INTEGER NOT NULL DEFAULT 0,
        conversation_id TEXT, checkpoint_id TEXT, job_type TEXT NOT NULL DEFAULT 'response');
        CREATE INDEX IF NOT EXISTS improvement_ready ON improvement_jobs(status,created_ms);
        CREATE INDEX IF NOT EXISTS improvement_origin ON improvement_jobs(origin_run);
        CREATE TRIGGER IF NOT EXISTS improvement_created AFTER INSERT ON improvement_jobs BEGIN
        INSERT INTO trace_events(run_id,kind,at_ms,data) VALUES(NEW.origin_run,'improvement.changed',NEW.updated_ms,json_object('job_id',NEW.id,'status',NEW.status)); END;
        CREATE TRIGGER IF NOT EXISTS improvement_updated AFTER UPDATE ON improvement_jobs BEGIN
        INSERT INTO trace_events(run_id,kind,at_ms,data) VALUES(NEW.origin_run,'improvement.changed',NEW.updated_ms,json_object('job_id',NEW.id,'status',NEW.status)); END;")?;
    for (column, kind) in [
        ("conversation_id", "TEXT"),
        ("checkpoint_id", "TEXT"),
        ("job_type", "TEXT NOT NULL DEFAULT 'response'"),
        // Old jobs remain explicitly review-only. New discretionary jobs may
        // persist a private-auto-apply intent, but the authority is always
        // revalidated inside the final mutation transaction.
        ("auto_apply_private", "INTEGER NOT NULL DEFAULT 0"),
    ] {
        ensure_column(conn, "improvement_jobs", column, kind)?;
    }
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS improvement_conversation_state(
        conversation_id TEXT NOT NULL, workforce_id TEXT NOT NULL, owner_key_id TEXT NOT NULL,
        user_turns INTEGER NOT NULL DEFAULT 0, next_turn_at INTEGER NOT NULL,
        next_elapsed_ms INTEGER, token_total INTEGER, last_screened_turn INTEGER NOT NULL DEFAULT 0,
        updated_ms INTEGER NOT NULL, epoch INTEGER NOT NULL DEFAULT -1,
        PRIMARY KEY(conversation_id,workforce_id,owner_key_id));
        CREATE TABLE IF NOT EXISTS improvement_turns(
        id INTEGER PRIMARY KEY, conversation_id TEXT NOT NULL, workforce_id TEXT NOT NULL,
        owner_key_id TEXT NOT NULL, run_id TEXT NOT NULL, request_id TEXT NOT NULL,
        prompt TEXT NOT NULL, usage_tokens INTEGER, usage_known INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL, created_ms INTEGER NOT NULL, epoch INTEGER NOT NULL DEFAULT -1,
        UNIQUE(conversation_id,workforce_id,owner_key_id,run_id));
        CREATE INDEX IF NOT EXISTS improvement_turns_scope_order
        ON improvement_turns(conversation_id,workforce_id,owner_key_id,id);
        CREATE TABLE IF NOT EXISTS improvement_checkpoints(
        id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, workforce_id TEXT NOT NULL,
        owner_key_id TEXT NOT NULL, origin_request TEXT NOT NULL, origin_run TEXT NOT NULL,
        through_turn INTEGER NOT NULL, triggers TEXT NOT NULL, status TEXT NOT NULL,
        screener TEXT, outcome TEXT, created_ms INTEGER NOT NULL, updated_ms INTEGER NOT NULL,
        epoch INTEGER NOT NULL DEFAULT 0);
        CREATE INDEX IF NOT EXISTS improvement_checkpoint_ready
        ON improvement_checkpoints(status,created_ms);
        CREATE TABLE IF NOT EXISTS improvement_suggestions(
        id TEXT PRIMARY KEY, job_id TEXT NOT NULL, checkpoint_id TEXT NOT NULL,
        conversation_id TEXT NOT NULL, workforce_id TEXT NOT NULL, owner_key_id TEXT NOT NULL,
        entity_kind TEXT NOT NULL, entity_id TEXT NOT NULL, action TEXT NOT NULL,
        proposal TEXT NOT NULL, before_definition TEXT NOT NULL, status TEXT NOT NULL,
        created_ms INTEGER NOT NULL, applied_ms INTEGER, application_request TEXT, share_id TEXT,
        declassified_ms INTEGER, idempotency_key TEXT);
        CREATE INDEX IF NOT EXISTS improvement_suggestion_owner
        ON improvement_suggestions(workforce_id,owner_key_id,status,created_ms);
        CREATE TABLE IF NOT EXISTS improvement_specialists(
        id TEXT PRIMARY KEY, definition TEXT NOT NULL, created_ms INTEGER NOT NULL);",
    )?;
    ensure_column(conn, "improvement_suggestions", "share_id", "TEXT")?;
    ensure_column(conn, "improvement_suggestions", "idempotency_key", "TEXT")?;
    ensure_column(
        conn,
        "improvement_suggestions",
        "declassified_ms",
        "INTEGER",
    )?;
    ensure_column(
        conn,
        "improvement_conversation_state",
        "epoch",
        "INTEGER NOT NULL DEFAULT -1",
    )?;
    ensure_column(
        conn,
        "improvement_turns",
        "epoch",
        "INTEGER NOT NULL DEFAULT -1",
    )?;
    let migration = conn.unchecked_transaction()?;
    let checkpoint_epoch_added = ensure_column(
        &migration,
        "improvement_checkpoints",
        "epoch",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    if checkpoint_epoch_added {
        let now = now_ms();
        migration.execute(
            "UPDATE improvement_checkpoints SET status='cancelled',outcome=?1,updated_ms=?2
             WHERE status IN ('queued','running','triggered')",
            params![
                json!({"reason":"legacy checkpoint had no durable reset epoch"}).to_string(),
                now
            ],
        )?;
        migration.execute(
            "UPDATE improvement_jobs SET status='cancelled',outcome=?1,updated_ms=?2
             WHERE status IN ('queued','running') AND checkpoint_id IN
               (SELECT id FROM improvement_checkpoints WHERE status='cancelled')",
            params![
                json!({"reason":"legacy checkpoint had no durable reset epoch"}).to_string(),
                now
            ],
        )?;
    }
    migration.commit()?;
    conn.execute(
        "UPDATE improvement_suggestions SET share_id=lower(hex(randomblob(16))) WHERE share_id IS NULL",
        [],
    )?;
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS improvement_suggestion_share_id
         ON improvement_suggestions(share_id) WHERE share_id IS NOT NULL;",
    )?;
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS improvement_consolidation_job_target
         ON improvement_suggestions(job_id,entity_kind,entity_id,action)
         WHERE action='consolidate';",
    )?;
    conn.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS improvement_suggestion_idempotency
         ON improvement_suggestions(idempotency_key) WHERE idempotency_key IS NOT NULL;",
    )?;
    // A held job has no durable evidence of response completion. Do not infer delivery.
    conn.execute("UPDATE improvement_jobs SET status='cancelled',outcome=?1,updated_ms=?2 WHERE status='held'",params![json!({"reason":"restart before response completion was recorded"}).to_string(),now_ms()])?;
    // An interrupted application is not a completed mutation. Return it to
    // pending before job reconciliation so the private-auto receipt path can see
    // it without another provider call.
    conn.execute(
        "UPDATE improvement_suggestions SET status='pending' WHERE status='applying'",
        [],
    )?;
    // Reconcile work that was running when the previous process stopped. A
    // committed suggestion receipt is the only durable evidence that evaluator
    // work finished, so reconcile from that receipt and never call a provider
    // during startup.
    conn.execute(
        "UPDATE improvement_jobs SET status='completed',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
           AND EXISTS(SELECT 1 FROM improvement_suggestions s
                      WHERE s.job_id=improvement_jobs.id)
           AND NOT EXISTS(SELECT 1 FROM improvement_suggestions s
                          WHERE s.job_id=improvement_jobs.id AND s.status<>'applied')",
        params![
            json!({
                "outcome":"completed",
                "reconciled":"applied_suggestion_receipt",
                "reason":"startup recovery observed a committed applied suggestion receipt"
            })
            .to_string(),
            now_ms()
        ],
    )?;
    // A crash after the evaluator receipt but before its private auto-apply
    // leaves a durable pending suggestion. Requeue only jobs carrying the new,
    // explicit intent marker; the worker applies that receipt without repeating
    // evaluator inference. Legacy and shared-workforce suggestions stay put.
    conn.execute(
        "UPDATE improvement_jobs SET status='queued',outcome=NULL,updated_ms=?1
         WHERE status='running' AND auto_apply_private=1
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
           AND EXISTS(SELECT 1 FROM improvement_suggestions s
             WHERE s.job_id=improvement_jobs.id AND s.status='pending')",
        [now_ms()],
    )?;
    // Any other committed suggestion receipt is terminal as suggested; explicit
    // application remains a separate owner action and does not replay inference.
    conn.execute(
        "UPDATE improvement_jobs SET status='suggested',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
           AND EXISTS(SELECT 1 FROM improvement_suggestions s
                      WHERE s.job_id=improvement_jobs.id)",
        params![
            json!({
                "outcome":"suggested",
                "reconciled":"suggestion_receipt",
                "reason":"startup recovery observed a committed suggestion receipt"
            })
            .to_string(),
            now_ms()
        ],
    )?;
    // Running work with no committed receipt has an unknown outcome. It is
    // terminal and is never automatically retried or replayed.
    conn.execute(
        "UPDATE improvement_jobs SET status='interrupted',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![
            json!({
                "outcome":"unknown",
                "reason":"service stopped before a durable improvement receipt; work was not replayed"
            })
            .to_string(),
            now_ms()
        ],
    )?;
    // A running row from an expired reset epoch must not become current work.
    conn.execute(
        "UPDATE improvement_jobs SET status='cancelled',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch<>(SELECT value FROM improvement_epoch WHERE id=1)",
        params![
            json!({"reason":"improvement job belongs to an earlier reset epoch"}).to_string(),
            now_ms()
        ],
    )?;
    // A crash after the evaluator receipt but before its private auto-apply can
    // also leave a suggested job. Requeue only the explicit private intent with
    // a still-pending receipt.
    conn.execute(
        "UPDATE improvement_jobs SET status='queued',outcome=NULL,updated_ms=?1
         WHERE status='suggested' AND auto_apply_private=1
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
           AND EXISTS(SELECT 1 FROM improvement_suggestions s
             WHERE s.job_id=improvement_jobs.id AND s.status='pending')",
        [now_ms()],
    )?;
    // Pre-checkpoint discretionary queue entries have no captured conversation
    // evidence or checkpoint association, so they cannot safely enter the new
    // evaluator path after an upgrade. Retain their rows as cancelled history.
    conn.execute(
        "UPDATE improvement_jobs SET status='cancelled',outcome=?1,updated_ms=?2
         WHERE status='queued' AND job_type='response' AND checkpoint_id IS NULL
         AND entity_kind IN ('agent','workflow')",
        params![
            json!({"reason":"legacy discretionary job has no checkpoint evidence"}).to_string(),
            now_ms()
        ],
    )?;
    // A screener call that was running has no durable screening verdict. Mark it
    // interrupted/unknown instead of requeueing it for another provider call.
    conn.execute(
        "UPDATE improvement_checkpoints SET status='interrupted',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![
            json!({
                "outcome":"unknown",
                "reason":"service stopped before a durable screening receipt; work was not replayed"
            })
            .to_string(),
            now_ms()
        ],
    )?;
    conn.execute(
        "UPDATE improvement_checkpoints SET status='cancelled',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch<>(SELECT value FROM improvement_epoch WHERE id=1)",
        params![
            json!({"reason":"checkpoint belongs to an earlier reset epoch"}).to_string(),
            now_ms()
        ],
    )?;
    Ok(())
}

fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    kind: &str,
) -> rusqlite::Result<bool> {
    let columns = conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|name| name == column) {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"))?;
        return Ok(true);
    }
    Ok(false)
}

fn scope_value(value: Option<&str>) -> &str {
    value.unwrap_or("")
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    if value.len() <= max_bytes {
        return (value.to_owned(), false);
    }
    let marker = "…[truncated]";
    let prefix_budget = max_bytes.saturating_sub(marker.len());
    let mut boundary = prefix_budget.min(value.len());
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    (format!("{}{marker}", &value[..boundary]), true)
}

fn foreground_tokens(conn: &Connection, run: &str) -> Result<(Option<i64>, bool), String> {
    const MODEL_CALLS: &[&str] = &[
        "foreman_decision",
        "agent_call",
        "composition",
        "evaluation",
        "creation_planning",
        "meta_agent_creation",
        "meta_workflow_creation",
        "consolidation_review",
        "foreman_output_contract",
    ];
    let kinds = MODEL_CALLS
        .iter()
        .map(|kind| format!("'{kind}'"))
        .collect::<Vec<_>>()
        .join(",");
    let mut statement = conn
        .prepare(&format!(
            "SELECT usage FROM trace_spans WHERE run_id=?1 AND kind IN ({kinds}) ORDER BY started_ms,rowid"
        ))
        .map_err(|error| error.to_string())?;
    let usages = statement
        .query_map([run], |row| row.get::<_, Option<String>>(0))
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    if usages.is_empty() {
        return Ok((None, false));
    }
    let mut total = 0i64;
    for usage in usages {
        let Some(usage) = usage else {
            return Ok((None, false));
        };
        let usage: Value = serde_json::from_str(&usage).map_err(|error| error.to_string())?;
        let tokens = usage["total_tokens"].as_i64().or_else(|| {
            Some(
                usage["prompt_tokens"]
                    .as_i64()?
                    .checked_add(usage["completion_tokens"].as_i64()?)?,
            )
        });
        let Some(tokens) = tokens.filter(|tokens| *tokens >= 0) else {
            return Ok((None, false));
        };
        let Some(sum) = total.checked_add(tokens) else {
            return Ok((None, false));
        };
        total = sum;
    }
    Ok((Some(total), true))
}

#[allow(clippy::too_many_arguments)]
fn schedule_released_turn(
    conn: &Connection,
    conversation: &str,
    run: &str,
    request: &str,
    workforce: &str,
    owner: &str,
    epoch: i64,
    usage_tokens: Option<i64>,
    usage_known: bool,
    config: &ImprovementConfig,
) -> rusqlite::Result<()> {
    let now = now_ms();
    let current_epoch: i64 = conn.query_row(
        "SELECT value FROM improvement_epoch WHERE id=1",
        [],
        |row| row.get(0),
    )?;
    if current_epoch != epoch {
        return Ok(());
    }
    let initial_elapsed = config
        .elapsed
        .enabled
        .then_some(now.saturating_add(config.elapsed.interval_ms as i64));
    conn.execute(
        "INSERT INTO improvement_conversation_state(conversation_id,workforce_id,owner_key_id,
         user_turns,next_turn_at,next_elapsed_ms,token_total,last_screened_turn,updated_ms,epoch)
         VALUES(?1,?2,?3,0,?4,?5,0,0,?6,?7)
         ON CONFLICT(conversation_id,workforce_id,owner_key_id) DO UPDATE SET
           user_turns=0,next_turn_at=excluded.next_turn_at,next_elapsed_ms=excluded.next_elapsed_ms,
           token_total=0,last_screened_turn=0,updated_ms=excluded.updated_ms,epoch=excluded.epoch
         WHERE improvement_conversation_state.epoch<>excluded.epoch",
        params![
            conversation,
            workforce,
            owner,
            config.turns.initial_interval,
            initial_elapsed,
            now,
            epoch
        ],
    )?;
    let (state_epoch, user_turns, next_turn_at, next_elapsed_ms, token_total, last_screened): (
        i64,
        i64,
        i64,
        Option<i64>,
        Option<i64>,
        i64,
    ) = conn.query_row(
        "SELECT epoch,user_turns,next_turn_at,next_elapsed_ms,token_total,last_screened_turn
         FROM improvement_conversation_state WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3",
        params![conversation,workforce,owner],
        |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
    )?;
    if state_epoch != epoch {
        return Ok(());
    }
    let updated_turns = user_turns.saturating_add(1);
    let new_token_total = match (token_total, usage_known, usage_tokens) {
        (Some(previous), true, Some(tokens)) if tokens >= 0 => previous.checked_add(tokens),
        _ => None,
    };
    conn.execute(
        "UPDATE improvement_conversation_state SET user_turns=?4,token_total=?5,updated_ms=?6
         WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3",
        params![
            conversation,
            workforce,
            owner,
            updated_turns,
            new_token_total,
            now
        ],
    )?;

    let mut triggers = Vec::new();
    let turn_due = config.turns.enabled && updated_turns >= next_turn_at;
    if turn_due {
        triggers.push("turns");
    }
    let has_new_prompts = updated_turns > last_screened;
    let elapsed_due =
        config.elapsed.enabled && next_elapsed_ms.is_some_and(|deadline| deadline <= now);
    let elapsed_triggered = elapsed_due && has_new_prompts;
    if elapsed_triggered {
        triggers.push("elapsed");
    }
    let token_due = config.foreground_tokens.enabled
        && config.foreground_tokens.threshold.is_some_and(|threshold| {
            new_token_total
                .is_some_and(|total| u64::try_from(total).is_ok_and(|total| total >= threshold))
        });
    if token_due {
        triggers.push("foreground_tokens");
    }

    let next_elapsed = if config.elapsed.enabled {
        if next_elapsed_ms.is_none() || elapsed_triggered {
            Some(now.saturating_add(config.elapsed.interval_ms as i64))
        } else {
            next_elapsed_ms
        }
    } else {
        None
    };
    if triggers.is_empty() {
        conn.execute(
            "UPDATE improvement_conversation_state SET next_elapsed_ms=?4,updated_ms=?5
             WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3",
            params![conversation, workforce, owner, next_elapsed, now],
        )?;
        return Ok(());
    }

    let pending: Option<(String, String)> = conn
        .query_row(
            "SELECT id,triggers FROM improvement_checkpoints WHERE conversation_id=?1 AND workforce_id=?2
             AND owner_key_id=?3 AND status='queued'
             AND epoch=?4 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
             ORDER BY created_ms LIMIT 1",
            params![conversation,workforce,owner,epoch],
            |row| Ok((row.get(0)?,row.get(1)?)),
        )
        .optional()?;
    if let Some((id, previous)) = pending {
        let mut merged: Vec<String> = serde_json::from_str(&previous).unwrap_or_default();
        for trigger in &triggers {
            if !merged.iter().any(|current| current == trigger) {
                merged.push((*trigger).to_string());
            }
        }
        conn.execute(
            "UPDATE improvement_checkpoints SET through_turn=?2,triggers=?3,origin_request=?4,origin_run=?5,updated_ms=?6
             WHERE id=?1 AND status='queued'
               AND epoch=?7
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![id,updated_turns,json!(merged).to_string(),request,run,now,epoch],
        )?;
    } else {
        conn.execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,origin_request,
              origin_run,through_turn,triggers,status,created_ms,updated_ms,epoch)
              VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'queued',?9,?9,?10)",
            params![uuid::Uuid::new_v4().to_string(),conversation,workforce,owner,request,run,updated_turns,json!(triggers).to_string(),now,epoch],
        )?;
    }
    conn.execute(
        "UPDATE improvement_conversation_state SET next_turn_at=?4,
         next_elapsed_ms=?5,token_total=0,updated_ms=?6
         WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3",
        params![
            conversation,
            workforce,
            owner,
            if turn_due {
                updated_turns.saturating_add(config.turns.initial_interval as i64)
            } else {
                next_turn_at
            },
            next_elapsed,
            now
        ],
    )?;
    Ok(())
}

fn auto_apply_private_eligible(
    conn: &Connection,
    workforce_id: &str,
    source_key_id: &str,
) -> Result<bool, String> {
    conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM workforces w
           WHERE w.id=?1 AND w.visibility='private'
             AND (SELECT COUNT(*) FROM api_keys active
                  WHERE active.workforce_id=w.id AND active.revoked_ms IS NULL)=1
             AND EXISTS(SELECT 1 FROM api_keys source
               WHERE source.id=?2 AND source.workforce_id=w.id
                 AND source.revoked_ms IS NULL
                 AND source.improvement_contribute=1 AND source.roster_write=1))",
        params![workforce_id, source_key_id],
        |row| row.get(0),
    )
    .map_err(|error| error.to_string())
}

impl Portfolio {
    #[allow(clippy::too_many_arguments)]
    pub fn queue_improvement(
        &self,
        request: &str,
        kind: &str,
        entity: &str,
        definition: &Value,
        task: &str,
        result: &str,
        generation: u64,
    ) -> Result<(), String> {
        let policy_job = kind == "workforce";
        if !policy_job && let Some(auth) = &self.auth {
            auth.require(crate::auth::RequiredPermission::ImprovementContribute)
                .map_err(str::to_owned)?;
        }
        let (run, _) = self
            .trace_context(request)?
            .ok_or("improvement missing originating trace")?;
        let conn = self.conn.lock().unwrap();
        let conversation: Option<String> = conn
            .query_row(
                "SELECT conversation_id FROM trace_runs WHERE id=?1",
                [&run],
                |row| row.get(0),
            )
            .map_err(|error| error.to_string())?;
        if self.generation() != generation {
            return Err("reset before improvement capture".into());
        }
        let closed: bool = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM improvement_responses WHERE request_id=?1)",
                [request],
                |r| r.get(0),
            )
            .map_err(|e| e.to_string())?;
        if closed {
            return Err("response already closed; improvement capture rejected".into());
        }
        let auto_apply_private = if let Some(auth) = &self.auth {
            auth.workforce_id == scope_value(self.workforce_scope())
                && auth.key_id == scope_value(self.key_scope())
                && auto_apply_private_eligible(&conn, &auth.workforce_id, &auth.key_id)?
        } else {
            false
        };
        conn.execute("INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,auto_apply,auto_apply_private,conversation_id,job_type)
            VALUES(?1,?2,?3,?4,?5,?6,?7,?8,(SELECT value FROM improvement_epoch WHERE id=1),'held',?9,?9,?10,?11,0,?12,?13,?14)",params![uuid::Uuid::new_v4().to_string(),request,run,kind,entity,definition.to_string(),task,result,now_ms(),self.workforce_scope(),self.key_scope(),auto_apply_private,conversation,if policy_job{"policy"}else{"response"}]).map_err(|e|e.to_string())?;
        drop(conn);
        let _ = self.trace_bus.send(());
        Ok(())
    }

    pub fn release_improvements(&self, request: &str) -> Result<(), String> {
        self.release_improvements_with_config(request, &ImprovementConfig::default())
    }

    pub fn release_improvements_with_config(
        &self,
        request: &str,
        config: &ImprovementConfig,
    ) -> Result<(), String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|e| e.to_string())?;
        tx.execute(
            "INSERT OR IGNORE INTO improvement_responses(request_id,status,workforce_id,owner_key_id) VALUES(?1,'released',?2,?3)",
            params![request,self.workforce_scope(),self.key_scope()],
        )
        .map_err(|e| e.to_string())?;
        tx.execute("UPDATE improvement_jobs SET status=CASE WHEN job_type='policy' OR entity_kind='workforce' THEN 'queued' ELSE 'pending_checkpoint' END,updated_ms=?2 WHERE origin_request=?1 AND status='held' AND epoch=(SELECT value FROM improvement_epoch WHERE id=1) AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4) AND EXISTS(SELECT 1 FROM improvement_responses WHERE request_id=?1 AND status='released')",params![request,now_ms(),self.workforce_scope(),self.key_scope()]).map_err(|e|e.to_string())?;
        let turn: Option<(String, String, String, String, i64, Option<i64>, bool)> = tx
            .query_row(
                "SELECT conversation_id,run_id,workforce_id,owner_key_id,epoch,usage_tokens,usage_known
                 FROM improvement_turns WHERE request_id=?1 AND status='held'
                 AND workforce_id=?2 AND owner_key_id=?3 ORDER BY id LIMIT 1",
                params![
                    request,
                    scope_value(self.workforce_scope()),
                    scope_value(self.key_scope())
                ],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                    ))
                },
            )
            .optional()
            .map_err(|e| e.to_string())?;
        tx.execute(
            "UPDATE improvement_turns SET status='released' WHERE request_id=?1 AND status='held'
             AND workforce_id=?2 AND owner_key_id=?3",
            params![
                request,
                scope_value(self.workforce_scope()),
                scope_value(self.key_scope())
            ],
        )
        .map_err(|e| e.to_string())?;
        if let Some((conversation, run, workforce, owner, epoch, usage_tokens, usage_known)) = turn
        {
            schedule_released_turn(
                &tx,
                &conversation,
                &run,
                request,
                &workforce,
                &owner,
                epoch,
                usage_tokens,
                usage_known,
                config,
            )
            .map_err(|error| error.to_string())?;
        }
        tx.commit().map_err(|e| e.to_string())?;
        drop(conn);
        let _ = self.trace_bus.send(());
        Ok(())
    }

    /// Capture only user-authored prompt material for the conversation screener.
    /// The row remains held until the response body is published.
    pub fn capture_conversation_turn(&self, request: &str, payload: &Value) -> Result<(), String> {
        let (run, _) = self
            .trace_context(request)?
            .ok_or("improvement turn missing originating trace")?;
        let Some(prompt) = payload["messages"]
            .as_array()
            .into_iter()
            .flatten()
            .rev()
            .find(|message| message["role"] == "user")
            .map(|message| match &message["content"] {
                Value::String(text) => text.clone(),
                value => value.to_string(),
            })
        else {
            return Ok(());
        };
        let (prompt, prompt_truncated) = truncate_utf8(&prompt, MAX_CAPTURED_PROMPT_BYTES);
        let prompt = if prompt_truncated {
            format!("{prompt} [the captured prompt is byte-limited]")
        } else {
            prompt
        };
        let conn = self.conn.lock().unwrap();
        let (conversation, epoch): (Option<String>, i64) = conn
            .query_row(
                "SELECT r.conversation_id,e.epoch FROM trace_runs r
                 JOIN improvement_run_epochs e ON e.run_id=r.id WHERE r.id=?1",
                [&run],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(|error| error.to_string())?;
        let (tokens, known) = foreground_tokens(&conn, &run)?;
        let Some(conversation) = conversation else {
            return Ok(());
        };
        if let Some(auth) = &self.auth {
            auth.require(crate::auth::RequiredPermission::ImprovementContribute)
                .map_err(str::to_owned)?;
        }
        conn.execute(
            "INSERT OR IGNORE INTO improvement_turns(conversation_id,workforce_id,owner_key_id,run_id,
             request_id,prompt,usage_tokens,usage_known,status,created_ms,epoch)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'held',?9,?10)",
            params![
                conversation,
                scope_value(self.workforce_scope()),
                scope_value(self.key_scope()),
                run,
                request,
                prompt,
                tokens,
                known,
                now_ms(),
                epoch
            ],
        )
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    pub fn cancel_improvements(&self, request: &str) {
        let result = (|| -> rusqlite::Result<()> {
            let mut conn = self.conn.lock().unwrap();
            let tx = conn.transaction()?;
            tx.execute(
                "INSERT OR IGNORE INTO improvement_responses(request_id,status,workforce_id,owner_key_id) VALUES(?1,'cancelled',?2,?3)",
                params![request,self.workforce_scope(),self.key_scope()],
            )?;
            tx.execute("UPDATE improvement_jobs SET status='cancelled',updated_ms=?2,outcome=?3 WHERE origin_request=?1 AND status='held' AND (?4 IS NULL OR workforce_id=?4) AND (?5 IS NULL OR owner_key_id=?5)",params![request,now_ms(),json!({"reason":"response did not complete"}).to_string(),self.workforce_scope(),self.key_scope()])?;
            tx.execute(
                "UPDATE improvement_turns SET status='cancelled' WHERE request_id=?1 AND status='held'
                 AND workforce_id=?2 AND owner_key_id=?3",
                params![request,scope_value(self.workforce_scope()),scope_value(self.key_scope())],
            )?;
            tx.commit()
        })();
        if let Err(error) = result {
            tracing::error!(%error,"cancelling held improvements failed");
        }
        let _ = self.trace_bus.send(());
    }

    pub fn improvements(&self, run: &str) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        if self.workforce_scope().is_none() && self.key_scope().is_none() {
            list(&conn, run)
        } else {
            list_scoped(&conn, run, self.workforce_scope(), self.key_scope())
        }
    }

    pub fn suggestions(&self) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        suggestions_scoped(
            &conn,
            scope_value(self.workforce_scope()),
            scope_value(self.key_scope()),
            self.auth
                .as_ref()
                .is_some_and(|auth| auth.permissions.roster_read && auth.permissions.roster_write),
        )
    }
}

/// Explicit source-key release for model-generated revision or creation
/// requirements. Structural projection alone cannot establish that generated
/// content is free of private evidence, so it remains owner-only until released.
#[allow(dead_code)] // A transport owner wires this consent action to the authorized API.
pub fn declassify_suggestion(
    state: &Arc<AppState>,
    id: &str,
    request: &str,
) -> Result<Value, String> {
    let auth = state
        .auth
        .as_ref()
        .ok_or("source-key authentication is required to release a suggestion")?;
    auth.require(crate::auth::RequiredPermission::ImprovementContribute)
        .map_err(str::to_owned)?;
    let now = now_ms();
    let changed = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_suggestions SET declassified_ms=?4
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND action IN ('revise','create')
               AND status='pending' AND declassified_ms IS NULL",
            params![id, auth.workforce_id, auth.key_id, now],
        )
        .map_err(|error| error.to_string())?;
    if changed != 1 {
        return Err("unknown, non-pending, or already released suggestion".into());
    }
    state.portfolio.trace_event(
        request,
        "improvement.suggestion_declassified",
        &json!({"suggestion_id":id,"scope":"same-workforce roster writers","content":"proposal content released by the source key"}),
    )?;
    let _ = state.portfolio.trace_bus.send(());
    Ok(json!({"id":id,"status":"pending","declassified":true}))
}

/// Verify that the currently authorized evidence owner still controls the whole
/// target roster. Call this only with the transaction that will commit the
/// definition mutation; an earlier/preflight check is not an authorization.
pub(crate) fn require_auto_private_mutation(
    tx: &rusqlite::Transaction<'_>,
    workforce_id: &str,
    source_key_id: &str,
) -> Result<(), String> {
    if auto_apply_private_eligible(tx, workforce_id, source_key_id)? {
        Ok(())
    } else {
        Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into())
    }
}

fn require_auto_private_job(
    tx: &rusqlite::Transaction<'_>,
    authority: &ApplicationAuthority,
) -> Result<(), String> {
    let ApplicationAuthority::AutoPrivate {
        source_key_id,
        workforce_id,
        job_id,
    } = authority
    else {
        return Ok(());
    };
    require_auto_private_mutation(tx, workforce_id, source_key_id)?;
    let current: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_jobs
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND auto_apply_private=1 AND status IN ('running','suggested')
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
            params![job_id, workforce_id, source_key_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if current {
        Ok(())
    } else {
        Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into())
    }
}

fn require_auto_private_suggestion(
    tx: &rusqlite::Transaction<'_>,
    authority: &ApplicationAuthority,
    suggestion: &SuggestionForApplication,
) -> Result<(), String> {
    let ApplicationAuthority::AutoPrivate {
        source_key_id,
        workforce_id,
        job_id,
    } = authority
    else {
        return Ok(());
    };
    require_auto_private_job(tx, authority)?;
    let bound: bool = tx
        .query_row(
            "SELECT EXISTS(
               SELECT 1 FROM improvement_suggestions s
               JOIN improvement_jobs j ON j.id=s.job_id
               WHERE s.id=?1 AND s.job_id=?2 AND s.workforce_id=?3
                 AND s.owner_key_id=?4 AND s.entity_kind=?5 AND s.entity_id=?6
                 AND s.action=?7 AND s.proposal=?8 AND s.before_definition=?9
                 AND s.status='pending' AND j.workforce_id=?3 AND j.owner_key_id=?4
                 AND j.auto_apply_private=1
                 AND j.status IN ('running','suggested')
                 AND j.epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
            params![
                suggestion.id,
                job_id,
                workforce_id,
                source_key_id,
                suggestion.entity_kind,
                suggestion.entity_id,
                suggestion.action,
                suggestion.proposal,
                suggestion.before_definition
            ],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if bound {
        Ok(())
    } else {
        Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into())
    }
}

struct SuggestionForApplication {
    id: String,
    job_id: String,
    workforce_id: String,
    owner_key_id: String,
    entity_kind: String,
    entity_id: String,
    action: String,
    proposal: String,
    before_definition: String,
    status: String,
    declassified_ms: Option<i64>,
}

fn suggestion_for_application(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<SuggestionForApplication> {
    Ok(SuggestionForApplication {
        id: row.get(0)?,
        job_id: row.get(1)?,
        workforce_id: row.get(2)?,
        owner_key_id: row.get(3)?,
        entity_kind: row.get(4)?,
        entity_id: row.get(5)?,
        action: row.get(6)?,
        proposal: row.get(7)?,
        before_definition: row.get(8)?,
        status: row.get(9)?,
        declassified_ms: row.get(10)?,
    })
}

pub fn apply_suggestion(state: &Arc<AppState>, id: &str, request: &str) -> Result<Value, String> {
    apply_suggestion_with_authority(state, id, request, &ApplicationAuthority::Explicit)
}

fn suggestion_action_for_dispatch(
    state: &AppState,
    id: &str,
    authority: &ApplicationAuthority,
) -> Result<Option<String>, String> {
    let portfolio = &state.portfolio;
    let conn = portfolio.conn.lock().unwrap();
    let own = conn
        .query_row(
            "SELECT action FROM improvement_suggestions
             WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND status IN ('pending','applied')",
            params![
                id,
                scope_value(portfolio.workforce_scope()),
                scope_value(portfolio.key_scope())
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    if own.is_some() || matches!(authority, ApplicationAuthority::AutoPrivate { .. }) {
        return Ok(own);
    }
    conn.query_row(
        "SELECT action FROM improvement_suggestions
         WHERE share_id=?1 AND workforce_id=?2 AND owner_key_id<>?3
           AND status IN ('pending','applied')",
        params![
            id,
            scope_value(portfolio.workforce_scope()),
            scope_value(portfolio.key_scope())
        ],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(|error| error.to_string())
}

/// Async application boundary for explicit create proposals, whose generator
/// and validator are asynchronous. Action resolution reads metadata only; the
/// registry performs authorization, release, snapshot, and receipt checks before
/// any provider call and repeats them in its final registration transaction.
pub(crate) async fn apply_suggestion_async_with_authority(
    state: &Arc<AppState>,
    id: &str,
    request: &str,
    authority: &ApplicationAuthority,
) -> Result<Value, String> {
    let action = suggestion_action_for_dispatch(state, id, authority)?;
    if action.as_deref() == Some("create") {
        if matches!(authority, ApplicationAuthority::AutoPrivate { .. }) {
            return Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
        }
        return crate::registry::create_from_suggestion(state, request, id).await;
    }
    apply_suggestion_with_authority(state, id, request, authority)
}

pub(crate) fn apply_suggestion_with_authority(
    state: &Arc<AppState>,
    id: &str,
    request: &str,
    authority: &ApplicationAuthority,
) -> Result<Value, String> {
    match authority {
        ApplicationAuthority::Explicit => {
            if let Some(auth) = &state.auth {
                auth.require(crate::auth::RequiredPermission::RosterWrite)
                    .map_err(str::to_owned)?;
            }
        }
        ApplicationAuthority::AutoPrivate {
            source_key_id,
            workforce_id,
            ..
        } => {
            let authorized_context = state.auth.as_ref().is_some_and(|auth| {
                auth.key_id == *source_key_id
                    && auth.workforce_id == *workforce_id
                    && auth.permissions.improvement_contribute
                    && auth.permissions.roster_write
            });
            if !authorized_context
                || state.portfolio.key_scope() != Some(source_key_id.as_str())
                || state.portfolio.workforce_scope() != Some(workforce_id.as_str())
            {
                return Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
            }
        }
    }
    let portfolio = &state.portfolio;
    let generation = portfolio.generation();
    let suggestion = {
        let conn = portfolio.conn.lock().unwrap();
        let own = conn
            .query_row(
                "SELECT id,job_id,workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,declassified_ms
                 FROM improvement_suggestions WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3",
                params![id,scope_value(portfolio.workforce_scope()),scope_value(portfolio.key_scope())],
                suggestion_for_application,
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if let Some(own) = own {
            own
        } else if matches!(authority, ApplicationAuthority::AutoPrivate { .. }) {
            // Background private application can never resolve a shared handle
            // owned by another key, even when the workforce was shared earlier.
            return Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
        } else {
            let shared = conn
                .query_row(
                    "SELECT id,job_id,workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,declassified_ms
                     FROM improvement_suggestions WHERE share_id=?1 AND workforce_id=?2
                       AND owner_key_id<>?3 AND status='pending'",
                    params![id,scope_value(portfolio.workforce_scope()),scope_value(portfolio.key_scope())],
                    suggestion_for_application,
                )
                .optional()
                .map_err(|error| error.to_string())?
                .ok_or("unknown suggestion")?;
            shared
        }
    };
    let kind = suggestion.entity_kind.as_str();
    let entity = suggestion.entity_id.as_str();
    let action = suggestion.action.as_str();
    let proposal_text = suggestion.proposal.as_str();
    let before_text = suggestion.before_definition.as_str();
    let status = suggestion.status.as_str();
    let cross_key = suggestion.owner_key_id != scope_value(portfolio.key_scope());
    if let ApplicationAuthority::AutoPrivate {
        source_key_id,
        workforce_id,
        job_id,
    } = authority
        && (cross_key
            || suggestion.owner_key_id != *source_key_id
            || suggestion.workforce_id != *workforce_id
            || suggestion.job_id != *job_id)
    {
        return Err(AUTO_PRIVATE_AUTHORIZATION_CHANGED.into());
    }
    if status == "applied" {
        return Ok(json!({"id":id,"status":"applied","entity_kind":kind,"entity_id":entity}));
    }
    if status != "pending" {
        return Err(format!("suggestion is {status}, not pending"));
    }
    if !["revise", "retire", "consolidate"].contains(&action) {
        return Err("suggestion action is not explicitly applicable".into());
    }
    if cross_key && action == "revise" && suggestion.declassified_ms.is_none() {
        return Err(
            "generated replacement remains private; source-key declassification is required".into(),
        );
    }
    if action == "consolidate" {
        let proposal: Value =
            serde_json::from_str(proposal_text).map_err(|error| error.to_string())?;
        let before: Value = serde_json::from_str(before_text).map_err(|error| error.to_string())?;
        let mut applied = crate::registry::apply_consolidation_suggestion(
            portfolio,
            request,
            &suggestion.id,
            &suggestion.owner_key_id,
            kind,
            entity,
            &before,
            &proposal,
            authority,
        )?;
        applied["id"] = json!(id);
        portfolio.trace_event(
            request,
            "improvement.suggestion_applied",
            &json!({"entity_kind":kind,"entity_id":entity,"action":"consolidate","survivor_id":applied["survivor_id"],"note":SHARED_SUGGESTION_NOTE}),
        )?;
        let _ = portfolio.trace_bus.send(());
        return Ok(applied);
    }
    let before: Value = serde_json::from_str(&before_text).map_err(|error| error.to_string())?;
    let proposal: Value =
        serde_json::from_str(&proposal_text).map_err(|error| error.to_string())?;
    let (replacement, current_before) = if kind == "agent" {
        let Some(current) = portfolio.get(entity).map_err(|error| error.to_string())? else {
            mark_stale(portfolio, &suggestion.id, &suggestion.owner_key_id)?;
            return Err("definition changed or was retired; suggestion is stale".into());
        };
        let current = json!(current);
        if current != before {
            mark_stale(portfolio, &suggestion.id, &suggestion.owner_key_id)?;
            return Err("definition changed after evaluation; suggestion is stale".into());
        }
        let replacement = if action == "revise" {
            let agent: crate::portfolio::NewAgent =
                serde_json::from_value(proposal["replacement"].clone())
                    .map_err(|error| format!("invalid evaluated agent replacement: {error}"))?;
            if agent.name.trim().is_empty()
                || agent.purpose.trim().is_empty()
                || agent.system_prompt.trim().is_empty()
            {
                return Err("evaluated agent replacement has blank required fields".into());
            }
            Some(
                json!({"provider":agent.provider,"id":entity,"name":agent.name,
                "purpose":agent.purpose,"system_prompt":agent.system_prompt,"model":agent.model}),
            )
        } else {
            None
        };
        (replacement, current)
    } else if kind == "workflow" {
        let Some(current) = portfolio.workflow_get(entity)? else {
            mark_stale(portfolio, &suggestion.id, &suggestion.owner_key_id)?;
            return Err("definition changed or was retired; suggestion is stale".into());
        };
        let current = serde_json::to_value(current).map_err(|error| error.to_string())?;
        if current != before {
            mark_stale(portfolio, &suggestion.id, &suggestion.owner_key_id)?;
            return Err("definition changed after evaluation; suggestion is stale".into());
        }
        let replacement = if action == "revise" {
            let mut workflow: crate::workflow::Workflow =
                serde_json::from_value(proposal["replacement"].clone())
                    .map_err(|error| error.to_string())?;
            workflow.id = entity.to_owned();
            crate::meta::validate_workflow_agents(state, &workflow)?;
            Some(serde_json::to_value(workflow).map_err(|error| error.to_string())?)
        } else {
            None
        };
        (replacement, current)
    } else {
        return Err("unsupported suggestion kind".into());
    };

    if portfolio.generation() != generation {
        return Err("reset during suggestion application".into());
    }
    let mut conn = portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    require_auto_private_suggestion(&tx, authority, &suggestion)?;
    let affected = if kind == "agent" {
        let agent = if action == "revise" {
            replacement.as_ref().unwrap()
        } else {
            &Value::Null
        };
        if action == "retire" {
            tx.execute(
                "UPDATE agents SET retired=1 WHERE id=?1 AND name=?2 AND purpose=?3 AND system_prompt=?4
                 AND model=?5 AND provider=?6 AND retired=0 AND (?7 IS NULL OR workforce_id=?7)",
                params![entity,before["name"].as_str(),before["purpose"].as_str(),before["system_prompt"].as_str(),before["model"].as_str(),before["provider"].as_str().unwrap_or(""),portfolio.workforce_scope()],
            )
        } else {
            tx.execute(
                "UPDATE agents SET name=?1,purpose=?2,system_prompt=?3,model=?4,provider=?5
                 WHERE id=?6 AND name=?7 AND purpose=?8 AND system_prompt=?9 AND model=?10 AND provider=?11
                 AND retired=0 AND (?12 IS NULL OR workforce_id=?12)",
                params![agent["name"].as_str(),agent["purpose"].as_str(),agent["system_prompt"].as_str(),agent["model"].as_str(),agent["provider"].as_str().unwrap_or(""),entity,before["name"].as_str(),before["purpose"].as_str(),before["system_prompt"].as_str(),before["model"].as_str(),before["provider"].as_str().unwrap_or(""),portfolio.workforce_scope()],
            )
        }
    } else {
        let before_workflow: crate::workflow::Workflow =
            serde_json::from_value(before.clone()).map_err(|error| error.to_string())?;
        let before_text = serde_json::to_string(&before_workflow).map_err(|error| error.to_string())?;
        if action == "retire" {
            tx.execute(
                "UPDATE workflows SET retired=1 WHERE id=?1 AND definition=?2 AND retired=0
                 AND (?3 IS NULL OR workforce_id=?3)",
                params![entity,before_text,portfolio.workforce_scope()],
            )
        } else {
            let after_workflow: crate::workflow::Workflow = serde_json::from_value(
                replacement.as_ref().unwrap().clone(),
            )
            .map_err(|error| error.to_string())?;
            tx.execute(
                "UPDATE workflows SET definition=?1 WHERE id=?2 AND definition=?3 AND retired=0
                 AND (?4 IS NULL OR workforce_id=?4)",
                params![serde_json::to_string(&after_workflow).map_err(|error|error.to_string())?,entity,before_text,portfolio.workforce_scope()],
            )
        }
    }
    .map_err(|error| error.to_string())?;
    if affected != 1 {
        tx.execute(
            "UPDATE improvement_suggestions SET status='stale' WHERE id=?1 AND status='pending'
             AND workforce_id=?2 AND owner_key_id=?3",
            params![
                suggestion.id,
                scope_value(portfolio.workforce_scope()),
                suggestion.owner_key_id
            ],
        )
        .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        return Err("definition changed after evaluation; suggestion is stale".into());
    }
    let before_version = crate::observe::snapshot_conn(
        &tx,
        &entity,
        &kind,
        &current_before,
        portfolio.workforce_scope(),
    )
    .map_err(|error| error.to_string())?;
    let after_version = replacement
        .as_ref()
        .map(|after| {
            crate::observe::snapshot_conn(&tx, &entity, &kind, after, portfolio.workforce_scope())
        })
        .transpose()
        .map_err(|error| error.to_string())?;
    let (run, span): (String, String) = tx
        .query_row(
            "SELECT q.run_id,q.root_span FROM trace_requests q JOIN trace_runs r ON r.id=q.run_id
             WHERE q.id=?1 AND (?2 IS NULL OR r.workforce_id=?2) AND (?3 IS NULL OR r.owner_key_id=?3)",
            params![request,portfolio.workforce_scope(),portfolio.key_scope()],
            |row| Ok((row.get(0)?,row.get(1)?)),
        )
        .map_err(|error| error.to_string())?;
    let audit_verdict = if let Some(replacement) = replacement.as_ref() {
        json!({"action":action,"reason":SHARED_SUGGESTION_NOTE,"replacement":replacement})
    } else {
        json!({"action":action,"reason":SHARED_SUGGESTION_NOTE})
    };
    tx.execute(
        "INSERT INTO evaluations(request_id,entity_id,entity_kind,verdict,before_definition,after_definition,
         applied,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6,1,?7,?8)",
        params![request,entity,kind,audit_verdict.to_string(),before.to_string(),replacement.as_ref().map(Value::to_string),portfolio.workforce_scope(),portfolio.key_scope()],
    )
    .map_err(|error| error.to_string())?;
    tx.execute(
        "INSERT INTO definition_changes(entity_id,kind,before_version,after_version,run_id,span_id,action,evidence,
         applied,at_ms,workforce_id,owner_key_id) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,1,?9,?10,?11)",
        params![entity,kind,before_version,after_version,run,span,format!("suggestion_{action}"),json!({"note":SHARED_SUGGESTION_NOTE}).to_string(),now_ms(),portfolio.workforce_scope(),portfolio.key_scope()],
    )
    .map_err(|error| error.to_string())?;
    let changed = tx.execute(
        "UPDATE improvement_suggestions SET status='applied',applied_ms=?3,application_request=NULL
         WHERE id=?1 AND status='pending' AND workforce_id=?2 AND owner_key_id=?4",
        params![suggestion.id,scope_value(portfolio.workforce_scope()),now_ms(),suggestion.owner_key_id],
    )
    .map_err(|error| error.to_string())?;
    if changed != 1 {
        return Err("suggestion was already claimed".into());
    }
    if let ApplicationAuthority::AutoPrivate {
        job_id,
        source_key_id,
        workforce_id,
    } = authority
    {
        let pending_suggestions = {
            let mut statement = tx
                .prepare(
                    "SELECT id FROM improvement_suggestions WHERE job_id=?1 AND workforce_id=?2
                     AND owner_key_id=?3 AND status='pending' ORDER BY created_ms,id",
                )
                .map_err(|error| error.to_string())?;
            statement
                .query_map(params![job_id, workforce_id, source_key_id], |row| {
                    row.get(0)
                })
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<String>, _>>()
                .map_err(|error| error.to_string())?
        };
        let all_applied = pending_suggestions.is_empty();
        let outcome = json!({
            "suggestion_id":suggestion.id,
            "entity_kind":kind,
            "entity_id":entity,
            "action":action,
            "applied":all_applied,
            "pending_suggestion_ids":pending_suggestions
        });
        let completed = tx
            .execute(
                "UPDATE improvement_jobs SET status=?4,outcome=?5,updated_ms=?6
                 WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
                   AND auto_apply_private=1 AND status IN ('running','suggested')
                   AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
                params![
                    job_id,
                    workforce_id,
                    source_key_id,
                    if all_applied {
                        "completed"
                    } else {
                        "suggested"
                    },
                    outcome.to_string(),
                    now_ms()
                ],
            )
            .map_err(|error| error.to_string())?;
        if completed != 1 {
            return Err("automatic application lost its job receipt or reset epoch".into());
        }
    }
    tx.commit().map_err(|error| error.to_string())?;
    drop(conn);
    portfolio.trace_event(
        request,
        "improvement.suggestion_applied",
        &json!({"entity_kind":kind,"entity_id":entity,"action":action,"note":SHARED_SUGGESTION_NOTE}),
    )?;
    let _ = portfolio.trace_bus.send(());
    Ok(
        json!({"id":id,"status":"applied","entity_kind":kind,"entity_id":entity,"action":action,"applied":true}),
    )
}

fn mark_stale(portfolio: &Portfolio, id: &str, owner_key_id: &str) -> Result<(), String> {
    portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_suggestions SET status='stale' WHERE id=?1 AND status='pending'
             AND workforce_id=?2 AND owner_key_id=?3",
            params![id, scope_value(portfolio.workforce_scope()), owner_key_id],
        )
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub fn list(conn: &Connection, run: &str) -> Result<Value, String> {
    let mut stmt=conn.prepare("SELECT id,entity_kind,entity_id,status,created_ms,updated_ms,evaluation_run,outcome,attempts FROM improvement_jobs WHERE origin_run=?1 ORDER BY created_ms,rowid").map_err(|e|e.to_string())?;
    let rows=stmt.query_map([run],|r|Ok(json!({"id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"entity_id":r.get::<_,String>(2)?,"status":r.get::<_,String>(3)?,"created_ms":r.get::<_,i64>(4)?,"updated_ms":r.get::<_,i64>(5)?,"evaluation_run":r.get::<_,Option<String>>(6)?,"outcome":r.get::<_,Option<String>>(7)?.and_then(|s|serde_json::from_str::<Value>(&s).ok()),"attempts":r.get::<_,i64>(8)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
    Ok(json!(rows))
}

pub fn list_scoped(
    conn: &Connection,
    run: &str,
    workforce: Option<&str>,
    owner: Option<&str>,
) -> Result<Value, String> {
    let mut stmt=conn.prepare("SELECT id,entity_kind,entity_id,status,created_ms,updated_ms,evaluation_run,outcome,attempts FROM improvement_jobs WHERE origin_run=?1 AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3) ORDER BY created_ms,rowid").map_err(|e|e.to_string())?;
    let rows=stmt.query_map(params![run,workforce,owner],|r|Ok(json!({"id":r.get::<_,String>(0)?,"kind":r.get::<_,String>(1)?,"entity_id":r.get::<_,String>(2)?,"status":r.get::<_,String>(3)?,"created_ms":r.get::<_,i64>(4)?,"updated_ms":r.get::<_,i64>(5)?,"evaluation_run":r.get::<_,Option<String>>(6)?,"outcome":r.get::<_,Option<String>>(7)?.and_then(|s|serde_json::from_str::<Value>(&s).ok()),"attempts":r.get::<_,i64>(8)?}))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
    Ok(json!(rows))
}

fn suggestions_scoped(
    conn: &Connection,
    workforce: &str,
    owner: &str,
    include_shared: bool,
) -> Result<Value, String> {
    let mut statement = conn
        .prepare(
            "SELECT id,job_id,checkpoint_id,entity_kind,entity_id,action,proposal,before_definition,status,created_ms,applied_ms,declassified_ms
             FROM improvement_suggestions WHERE workforce_id=?1 AND owner_key_id=?2
             ORDER BY created_ms DESC,id",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(params![workforce, owner], |row| {
            Ok(json!({
                "id":row.get::<_,String>(0)?,
                "job_id":row.get::<_,String>(1)?,
                "checkpoint_id":row.get::<_,String>(2)?,
                "kind":row.get::<_,String>(3)?,
                "entity_id":row.get::<_,String>(4)?,
                "action":row.get::<_,String>(5)?,
                "proposal":serde_json::from_str::<Value>(&row.get::<_,String>(6)?).unwrap_or(Value::Null),
                "before":serde_json::from_str::<Value>(&row.get::<_,String>(7)?).unwrap_or(Value::Null),
                "status":row.get::<_,String>(8)?,
                "created_ms":row.get::<_,i64>(9)?,
                "applied_ms":row.get::<_,Option<i64>>(10)?,
                "declassified":row.get::<_,Option<i64>>(11)?.is_some()
            }))
        })
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut suggestions = rows;
    if include_shared {
        let mut statement = conn
            .prepare(
                "SELECT share_id,entity_kind,entity_id,action,proposal,declassified_ms
                 FROM improvement_suggestions
                 WHERE workforce_id=?1 AND owner_key_id<>?2 AND status='pending'
                   AND share_id IS NOT NULL AND action IN ('revise','retire','consolidate','create')
                  ORDER BY created_ms DESC,id",
            )
            .map_err(|error| error.to_string())?;
        let shared = statement
            .query_map(params![workforce, owner], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, Option<i64>>(5)?.is_some(),
                ))
            })
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        for (share_id, kind, entity_id, action, proposal, declassified) in shared {
            // Generated replacement text and creation requirements are not
            // structurally sanitized; content remains withheld until the source
            // key explicitly releases it.
            let (proposed_definition, requirements, declassification_required) =
                if action == "revise" && declassified {
                    let Ok(proposal) = serde_json::from_str::<Value>(&proposal) else {
                        continue;
                    };
                    let Some(definition) =
                        shared_replacement_after_release(&kind, &entity_id, &proposal)
                    else {
                        continue;
                    };
                    (definition, Value::Null, false)
                } else if action == "consolidate" {
                    let survivor = serde_json::from_str::<Value>(&proposal)
                        .ok()
                        .and_then(|value| {
                            value
                                .get("survivor_id")
                                .and_then(Value::as_str)
                                .map(|id| json!({"survivor_id":id}))
                        })
                        .unwrap_or(Value::Null);
                    (survivor, Value::Null, false)
                } else if action == "create" {
                    let Ok(proposal) = serde_json::from_str::<Value>(&proposal) else {
                        continue;
                    };
                    if proposal["action"] != "create" || proposal["kind"] != kind {
                        continue;
                    }
                    let Some(requirements) = proposal.get("requirements") else {
                        continue;
                    };
                    (
                        Value::Null,
                        if declassified {
                            requirements.clone()
                        } else {
                            Value::Null
                        },
                        !declassified,
                    )
                } else {
                    (Value::Null, Value::Null, false)
                };
            let mut shared_suggestion = json!({
                "id":share_id,
                "kind":kind,
                "entity_id":entity_id,
                "action":action,
                "proposed_definition":proposed_definition,
                "declassification_required":declassification_required
                    || (action == "revise" && !declassified),
                "note":SHARED_SUGGESTION_NOTE,
                "status":"pending"
            });
            if action == "create" {
                shared_suggestion["requirements"] = requirements;
            }
            suggestions.push(shared_suggestion);
        }
    }
    Ok(json!(suggestions))
}

/// Shape validation/projection only, not a content sanitizer. Callers must first
/// verify source-key declassification for the entire generated replacement.
fn shared_replacement_after_release(
    kind: &str,
    entity_id: &str,
    proposal: &Value,
) -> Option<Value> {
    let replacement = proposal.get("replacement")?.clone();
    match kind {
        "agent" => {
            let agent: crate::portfolio::NewAgent = serde_json::from_value(replacement).ok()?;
            Some(json!({
                "provider":agent.provider,
                "id":entity_id,
                "name":agent.name,
                "purpose":agent.purpose,
                "system_prompt":agent.system_prompt,
                "model":agent.model
            }))
        }
        "workflow" => {
            let mut workflow: crate::workflow::Workflow =
                serde_json::from_value(replacement).ok()?;
            workflow.id = entity_id.to_owned();
            workflow.validate().ok()?;
            serde_json::to_value(workflow).ok()
        }
        _ => None,
    }
}

/// Drop cancels held jobs even when an unpolled response body is discarded.
pub struct ResponseGate {
    portfolio: Arc<Portfolio>,
    request: String,
    config: ImprovementConfig,
    finished: bool,
    cancel_on_drop: bool,
}
impl ResponseGate {
    pub fn new(portfolio: Arc<Portfolio>, request: String, config: ImprovementConfig) -> Self {
        Self {
            portfolio,
            request,
            config,
            finished: false,
            cancel_on_drop: true,
        }
    }
    pub fn complete(&mut self) {
        match self
            .portfolio
            .release_improvements_with_config(&self.request, &self.config)
        {
            Ok(()) => self.finished = true,
            Err(error) => tracing::error!(%error,"releasing post-response improvements failed"),
        }
    }

    /// Keep completed non-streaming async work held for its later result response.
    pub fn defer(&mut self) {
        self.finished = true;
    }

    /// Preserve held work so a client can retry an unconsumed result response.
    pub fn retain_on_drop(&mut self) {
        self.cancel_on_drop = false;
    }
}
impl Drop for ResponseGate {
    fn drop(&mut self) {
        if !self.finished && self.cancel_on_drop {
            self.portfolio.cancel_improvements(&self.request);
        }
    }
}

struct Job {
    id: String,
    origin_run: String,
    kind: String,
    entity: String,
    definition: Value,
    task: String,
    result: String,
    request: String,
    generation: u64,
    epoch: i64,
    workforce_id: Option<String>,
    owner_key_id: Option<String>,
    conversation_id: Option<String>,
    checkpoint_id: Option<String>,
    job_type: String,
    auto_apply_private: bool,
}

pub(crate) struct Checkpoint {
    pub(crate) id: String,
    pub(crate) conversation_id: String,
    pub(crate) origin_request: String,
    pub(crate) origin_run: String,
    pub(crate) through_turn: i64,
    pub(crate) triggers: String,
    pub(crate) workforce_id: String,
    pub(crate) owner_key_id: String,
    pub(crate) epoch: i64,
}

fn claim(state: &AppState, policy_only: bool) -> Result<Option<Job>, String> {
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|e| e.to_string())?;
    let condition = if policy_only {
        "(job_type='policy' OR entity_kind='workforce')"
    } else {
        "((job_type='response' AND checkpoint_id IS NOT NULL AND entity_kind IN ('agent','workflow'))
          OR job_type='consolidation')"
    };
    let sql = format!(
        "SELECT id,entity_kind,entity_id,definition,task,result,origin_run,workforce_id,owner_key_id,
         conversation_id,checkpoint_id,job_type,epoch,auto_apply_private FROM improvement_jobs
         WHERE status='queued' AND epoch=(SELECT value FROM improvement_epoch WHERE id=1) AND {condition}
          ORDER BY CASE WHEN job_type='response' THEN 0 ELSE 1 END,created_ms,rowid LIMIT 1"
    );
    let row = tx
        .query_row(&sql, [], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
                r.get::<_, Option<String>>(7)?,
                r.get::<_, Option<String>>(8)?,
                r.get::<_, Option<String>>(9)?,
                r.get::<_, Option<String>>(10)?,
                r.get::<_, String>(11)?,
                r.get::<_, i64>(12)?,
                r.get::<_, bool>(13)?,
            ))
        })
        .optional()
        .map_err(|e| e.to_string())?;
    let Some((
        id,
        kind,
        entity,
        definition,
        task,
        result,
        origin_run,
        workforce_id,
        owner_key_id,
        conversation_id,
        checkpoint_id,
        job_type,
        epoch,
        auto_apply_private,
    )) = row
    else {
        return Ok(None);
    };
    let definition = serde_json::from_str(&definition).map_err(|e| e.to_string())?;
    let request = format!("improvement-{}", uuid::Uuid::new_v4());
    let claimed = tx
        .execute(
            "UPDATE improvement_jobs SET status='running',evaluation_request=?2,
         attempts=attempts+1,updated_ms=?3 WHERE id=?1 AND status='queued' AND epoch=?4
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![id, request, now_ms(), epoch],
        )
        .map_err(|error| error.to_string())?;
    if claimed != 1 {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(None);
    }
    let generation = state.portfolio.generation();
    tx.commit().map_err(|e| e.to_string())?;
    Ok(Some(Job {
        id,
        origin_run,
        kind,
        entity,
        definition,
        task,
        result,
        request,
        generation,
        epoch,
        workforce_id,
        owner_key_id,
        conversation_id,
        checkpoint_id,
        job_type,
        auto_apply_private,
    }))
}

fn claim_checkpoint(state: &AppState) -> Result<Option<Checkpoint>, String> {
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let epoch: i64 = tx
        .query_row(
            "SELECT value FROM improvement_epoch WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    tx.execute(
        "UPDATE improvement_checkpoints SET status='cancelled',outcome=?1,updated_ms=?2
         WHERE status IN ('queued','running','triggered') AND epoch<>?3",
        params![
            json!({"reason":"checkpoint belongs to an earlier reset epoch"}).to_string(),
            now_ms(),
            epoch
        ],
    )
    .map_err(|error| error.to_string())?;
    let row = tx
        .query_row(
            "SELECT id,conversation_id,origin_request,origin_run,through_turn,triggers,workforce_id,owner_key_id,epoch
             FROM improvement_checkpoints WHERE status='queued' AND epoch=?1 ORDER BY created_ms LIMIT 1",
            [epoch],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                    row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?,
                    row.get::<_, String>(7)?,
                    row.get::<_, i64>(8)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some((
        id,
        conversation_id,
        origin_request,
        origin_run,
        through_turn,
        triggers,
        workforce_id,
        owner_key_id,
        epoch,
    )) = row
    else {
        return Ok(None);
    };
    let claimed = tx
        .execute(
            "UPDATE improvement_checkpoints SET status='running',updated_ms=?2
         WHERE id=?1 AND status='queued' AND epoch=?3
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![id, now_ms(), epoch],
        )
        .map_err(|error| error.to_string())?;
    if claimed != 1 {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(None);
    }
    tx.commit().map_err(|error| error.to_string())?;
    Ok(Some(Checkpoint {
        id,
        conversation_id,
        origin_request,
        origin_run,
        through_turn,
        triggers,
        workforce_id,
        owner_key_id,
        epoch,
    }))
}

/// Enqueue elapsed checkpoints from the worker clock, not from a later response.
/// The state lock makes deadline advancement, trigger coalescing, and checkpoint
/// insertion one operation with response-release scheduling.
fn enqueue_due_elapsed_checkpoint(state: &AppState) -> Result<bool, String> {
    let config = &state.config.improvement.elapsed;
    if !config.enabled {
        return Ok(false);
    }
    let now = now_ms();
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let due = tx
        .query_row(
            "SELECT s.conversation_id,s.workforce_id,s.owner_key_id,s.user_turns,s.epoch
             FROM improvement_conversation_state s
             WHERE s.next_elapsed_ms IS NOT NULL AND s.next_elapsed_ms<=?1
               AND s.user_turns>s.last_screened_turn
               AND EXISTS(SELECT 1 FROM improvement_turns t
                 WHERE t.conversation_id=s.conversation_id AND t.workforce_id=s.workforce_id
                    AND t.owner_key_id=s.owner_key_id AND t.epoch=s.epoch AND t.status='released')
               AND s.epoch=(SELECT value FROM improvement_epoch WHERE id=1)
             ORDER BY s.next_elapsed_ms,s.updated_ms,s.conversation_id LIMIT 1",
            [now],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some((conversation, workforce, owner, through_turn, epoch)) = due else {
        return Ok(false);
    };
    let origin: Option<(String, String)> = tx
        .query_row(
            "SELECT request_id,run_id FROM improvement_turns
             WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND status='released'
               AND epoch=?4 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
             ORDER BY id DESC LIMIT 1",
            params![conversation, workforce, owner, epoch],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some((origin_request, origin_run)) = origin else {
        return Ok(false);
    };
    let pending: Option<(String, String)> = tx
        .query_row(
            "SELECT id,triggers FROM improvement_checkpoints
             WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND status='queued'
               AND epoch=?4 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
             ORDER BY created_ms,id LIMIT 1",
            params![conversation, workforce, owner, epoch],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    if let Some((id, previous)) = pending {
        let mut triggers: Vec<String> = serde_json::from_str(&previous).unwrap_or_default();
        if !triggers.iter().any(|trigger| trigger == "elapsed") {
            triggers.push("elapsed".into());
        }
        tx.execute(
            "UPDATE improvement_checkpoints SET through_turn=MAX(through_turn,?2),triggers=?3,
             origin_request=?4,origin_run=?5,updated_ms=?6 WHERE id=?1 AND status='queued'
               AND epoch=?7 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![
                id,
                through_turn,
                json!(triggers).to_string(),
                origin_request,
                origin_run,
                now,
                epoch
            ],
        )
        .map_err(|error| error.to_string())?;
    } else {
        tx.execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms,epoch)
              VALUES(?1,?2,?3,?4,?5,?6,?7,'[\"elapsed\"]','queued',?8,?8,
                ?9)",
            params![
                uuid::Uuid::new_v4().to_string(),
                conversation,
                workforce,
                owner,
                origin_request,
                origin_run,
                through_turn,
                now,
                epoch
            ],
        )
        .map_err(|error| error.to_string())?;
    }
    tx.execute(
        "UPDATE improvement_conversation_state SET next_elapsed_ms=?4,updated_ms=?5
         WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND epoch=?6
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![
            conversation,
            workforce,
            owner,
            now.saturating_add(config.interval_ms as i64),
            now,
            epoch
        ],
    )
    .map_err(|error| error.to_string())?;
    tx.commit().map_err(|error| error.to_string())?;
    let _ = state.portfolio.trace_bus.send(());
    Ok(true)
}

fn scoped_state(
    state: &Arc<AppState>,
    workforce: Option<&str>,
    owner: Option<&str>,
    require_write: bool,
    id: &str,
) -> Result<Option<Arc<AppState>>, String> {
    match (workforce, owner) {
        (None, None) => Ok(Some(state.clone())),
        (Some(workforce), Some(owner)) => {
            let Some(auth) = state
                .portfolio
                .auth_context_for_key_id(owner)
                .map_err(|error| error.to_string())?
            else {
                return Ok(None);
            };
            let allowed = auth.workforce_id == workforce
                && if require_write {
                    auth.permissions.roster_write
                } else {
                    auth.permissions.improvement_contribute
                };
            if allowed {
                Ok(Some(state.with_auth(&auth)))
            } else {
                Err(if require_write {
                    format!("policy work {id} requires current roster_write permission")
                } else {
                    format!("improvement {id} requires current improvement_contribute permission")
                })
            }
        }
        _ => Err(format!("invalid ownership context for improvement {id}")),
    }
}

struct CheckpointEvidence {
    prompts: Vec<Value>,
    execution: Value,
    coverage: Value,
}

fn checkpoint_context(
    portfolio: &Portfolio,
    checkpoint: &Checkpoint,
) -> Result<CheckpointEvidence, String> {
    let conn = portfolio.conn.lock().unwrap();
    let prompt_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM (SELECT id FROM improvement_turns WHERE conversation_id=?1
             AND workforce_id=?2 AND owner_key_id=?3 AND epoch=?4 AND status='released'
             ORDER BY id LIMIT ?5)",
            params![
                checkpoint.conversation_id,
                checkpoint.workforce_id,
                checkpoint.owner_key_id,
                checkpoint.epoch,
                checkpoint.through_turn
            ],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let latest_sample_count = MAX_PROMPT_SAMPLES - i64::from(prompt_count > MAX_PROMPT_SAMPLES);
    let mut prompt_statement = conn
        .prepare(
            "SELECT sample.turn_no,substr(sample.prompt,1,?6) FROM (
               SELECT t.id,t.prompt,
                 (SELECT COUNT(*) FROM improvement_turns prior WHERE prior.conversation_id=?1
                   AND prior.workforce_id=?2 AND prior.owner_key_id=?3 AND prior.epoch=?4
                   AND prior.status='released'
                   AND prior.id<=t.id) AS turn_no
               FROM (SELECT id,prompt FROM improvement_turns WHERE conversation_id=?1
                 AND workforce_id=?2 AND owner_key_id=?3 AND epoch=?4 AND status='released'
                 ORDER BY id LIMIT ?5) t ORDER BY t.id DESC LIMIT ?7
              ) sample ORDER BY sample.id",
        )
        .map_err(|error| error.to_string())?;
    let mut prompt_rows = prompt_statement
        .query_map(
            params![
                checkpoint.conversation_id,
                checkpoint.workforce_id,
                checkpoint.owner_key_id,
                checkpoint.epoch,
                checkpoint.through_turn,
                MAX_CAPTURED_PROMPT_BYTES as i64 + 1,
                latest_sample_count
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(prompt_statement);
    if prompt_count > MAX_PROMPT_SAMPLES {
        let first = conn
            .query_row(
                "SELECT t.id,substr(t.prompt,1,?5),
                 (SELECT COUNT(*) FROM improvement_turns prior WHERE prior.conversation_id=?1
                   AND prior.workforce_id=?2 AND prior.owner_key_id=?3 AND prior.epoch=?4
                   AND prior.status='released'
                   AND prior.id<=t.id)
                 FROM improvement_turns t WHERE t.conversation_id=?1 AND t.workforce_id=?2
                   AND t.owner_key_id=?3 AND t.epoch=?4 AND t.status='released' AND t.id IN
                    (SELECT id FROM improvement_turns WHERE conversation_id=?1 AND workforce_id=?2
                     AND owner_key_id=?3 AND epoch=?4 AND status='released' ORDER BY id LIMIT ?6)
                  ORDER BY t.id LIMIT 1",
                params![
                    checkpoint.conversation_id,
                    checkpoint.workforce_id,
                    checkpoint.owner_key_id,
                    checkpoint.epoch,
                    MAX_CAPTURED_PROMPT_BYTES as i64 + 1,
                    checkpoint.through_turn
                ],
                |row| Ok((row.get::<_, i64>(2)?, row.get::<_, String>(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        if let Some(first) = first {
            prompt_rows.insert(0, first);
        }
    }
    let mut prompt_truncated = false;
    let prompts = prompt_rows
        .into_iter()
        .map(|(turn, text)| {
            let (text, truncated) = truncate_utf8(&text, MAX_CAPTURED_PROMPT_BYTES);
            prompt_truncated |= truncated;
            json!({"turn":turn,"text":text,"truncated":truncated})
        })
        .collect::<Vec<_>>();

    let run_count: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT run_id) FROM (SELECT run_id FROM improvement_turns
             WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND epoch=?4
               AND status='released' ORDER BY id LIMIT ?5)",
            params![
                checkpoint.conversation_id,
                checkpoint.workforce_id,
                checkpoint.owner_key_id,
                checkpoint.epoch,
                checkpoint.through_turn
            ],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    let mut run_statement = conn
        .prepare(
            "SELECT run_id FROM (SELECT id,run_id FROM improvement_turns WHERE conversation_id=?1
             AND workforce_id=?2 AND owner_key_id=?3 AND epoch=?4 AND status='released'
             ORDER BY id LIMIT ?5) ORDER BY id DESC LIMIT ?6",
        )
        .map_err(|error| error.to_string())?;
    let run_ids = run_statement
        .query_map(
            params![
                checkpoint.conversation_id,
                checkpoint.workforce_id,
                checkpoint.owner_key_id,
                checkpoint.epoch,
                checkpoint.through_turn,
                MAX_RUN_SAMPLES
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(run_statement);

    let mut spans = Vec::new();
    let mut events = Vec::new();
    let mut spans_available = 0_i64;
    let mut events_available = 0_i64;
    for run in &run_ids {
        spans_available += conn
            .query_row(
                "SELECT COUNT(*) FROM trace_spans s JOIN trace_runs r ON r.id=s.run_id
                 WHERE s.run_id=?1 AND r.source!='improvement'",
                [run],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| error.to_string())?;
        events_available += conn
            .query_row(
                "SELECT COUNT(*) FROM trace_events WHERE run_id=?1
                 AND kind NOT IN ('model.delta','span.input')",
                [run],
                |row| row.get::<_, i64>(0),
            )
            .map_err(|error| error.to_string())?;
        let mut statement = conn
            .prepare(
                "SELECT kind,entity_id,status,substr(input,1,?2),substr(output,1,?2),usage,version_id
                 FROM (SELECT s.kind,s.entity_id,s.status,s.input,s.output,s.usage,s.version_id,
                   s.started_ms,s.rowid FROM trace_spans s JOIN trace_runs r ON r.id=s.run_id
                   WHERE s.run_id=?1 AND r.source!='improvement'
                   ORDER BY s.started_ms DESC,s.rowid DESC LIMIT ?3)
                 ORDER BY started_ms,rowid",
            )
            .map_err(|error| error.to_string())?;
        spans.extend(
            statement
                .query_map(
                    params![
                        run,
                        MAX_TRACE_FIELD_CHARS as i64 + 1,
                        MAX_SPAN_SAMPLES / MAX_RUN_SAMPLES
                    ],
                    |row| {
                        Ok(json!({
                            "kind":row.get::<_,String>(0)?,"entity_id":row.get::<_,String>(1)?,
                            "status":row.get::<_,String>(2)?,"input":parse_bounded_json(row.get(3)?),
                            "output":parse_bounded_json(row.get(4)?),"usage":parse_bounded_json(row.get(5)?),
                            "version_id":row.get::<_,Option<String>>(6)?
                        }))
                    },
                )
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?,
        );
        let mut statement = conn
            .prepare(
                "SELECT kind,substr(data,1,?2) FROM (
                   SELECT kind,data,seq FROM trace_events WHERE run_id=?1
                   AND kind NOT IN ('model.delta','span.input') ORDER BY seq DESC LIMIT ?3
                 ) ORDER BY seq",
            )
            .map_err(|error| error.to_string())?;
        events.extend(
            statement
                .query_map(
                    params![
                        run,
                        MAX_TRACE_FIELD_CHARS as i64 + 1,
                        MAX_EVENT_SAMPLES / MAX_RUN_SAMPLES
                    ],
                    |row| {
                        Ok(json!({"kind":row.get::<_,String>(0)?,"data":parse_bounded_json(row.get(1)?)}))
                    },
                )
                .map_err(|error| error.to_string())?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| error.to_string())?,
        );
    }
    let execution = json!({"runs":run_ids,"spans":spans,"events":events});
    let prompt_complete = prompt_count <= MAX_PROMPT_SAMPLES && !prompt_truncated;
    let coverage = json!({
        "complete":prompt_complete && run_count <= MAX_RUN_SAMPLES
            && spans_available == spans.len() as i64 && events_available == events.len() as i64,
        "prompts_available":prompt_count,
        "prompts_included":prompts.len(),
        "prompts_complete":prompt_complete,
        "runs_available":run_count,
        "runs_included":run_ids.len(),
        "spans_available_in_sampled_runs":spans_available,
        "spans_included":spans.len(),
        "events_available_in_sampled_runs":events_available,
        "events_included":events.len(),
        "trace_fields_may_be_truncated":true,
        "sampling":"first prompt plus most recent prompts; most recent 16 runs and up to four recent spans/events per run"
    });
    Ok(CheckpointEvidence {
        prompts,
        execution,
        coverage,
    })
}

fn parse_bounded_json(text: Option<String>) -> Value {
    let Some(text) = text else {
        return Value::Null;
    };
    let (bounded, truncated) = truncate_chars(&text, MAX_TRACE_FIELD_CHARS);
    if truncated {
        json!({"truncated":true,"prefix":bounded})
    } else {
        serde_json::from_str(&bounded).unwrap_or(json!(bounded))
    }
}

fn truncate_chars(value: &str, max_chars: usize) -> (String, bool) {
    let mut chars = value.chars();
    let prefix = chars.by_ref().take(max_chars).collect::<String>();
    let truncated = chars.next().is_some();
    if truncated {
        (format!("{prefix}…[truncated]"), true)
    } else {
        (prefix, false)
    }
}

fn bounded_json_input(mut value: Value, max_bytes: usize) -> String {
    if value.get("coverage").and_then(Value::as_object).is_none() {
        return serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
    }
    loop {
        sync_evidence_coverage(&mut value);
        if serde_json::to_vec(&value).map_or(0, |bytes| bytes.len()) <= max_bytes {
            break;
        }
        value["coverage"]["complete"] = Value::Bool(false);
        if let Some(items) = value
            .get_mut("conversation_execution")
            .and_then(|v| v.get_mut("events"))
            .and_then(Value::as_array_mut)
            && !items.is_empty()
        {
            items.remove(0);
            increment_coverage(&mut value, "events_omitted_from_context");
            continue;
        }
        if let Some(items) = value
            .get_mut("conversation_execution")
            .and_then(|v| v.get_mut("spans"))
            .and_then(Value::as_array_mut)
            && !items.is_empty()
        {
            items.remove(0);
            increment_coverage(&mut value, "spans_omitted_from_context");
            continue;
        }
        if let Some(items) = value.get_mut("user_prompts").and_then(Value::as_array_mut)
            && items.len() > 1
        {
            items.remove(1);
            increment_coverage(&mut value, "prompts_omitted_from_context");
            continue;
        }
        let mut shortened = false;
        for key in ["captured_result", "captured_task"] {
            if let Some(text) = value.get(key).and_then(Value::as_str) {
                if text.len() > 64 {
                    let (prefix, _) = truncate_utf8(text, text.len() / 2);
                    value[key] = json!(prefix);
                    value["captured_material_truncated"] = Value::Bool(true);
                    shortened = true;
                    break;
                }
            }
        }
        if shortened {
            continue;
        }
        if value
            .get("evaluated_definition")
            .is_some_and(|v| !v.is_null())
        {
            value["evaluated_definition"] = Value::Null;
            value["coverage"]["definition_omitted_from_context"] = Value::Bool(true);
            continue;
        }
        if let Some(items) = value.get_mut("user_prompts").and_then(Value::as_array_mut)
            && !items.is_empty()
        {
            items.pop();
            increment_coverage(&mut value, "prompts_omitted_from_context");
            continue;
        }
        break;
    }
    sync_evidence_coverage(&mut value);
    let text = serde_json::to_string(&value).unwrap_or_else(|_| "{}".into());
    if text.len() <= max_bytes {
        text
    } else {
        let mut coverage = value["coverage"].clone();
        coverage["complete"] = Value::Bool(false);
        for key in [
            "prompts_included",
            "runs_included",
            "spans_included",
            "events_included",
            "included",
        ] {
            if coverage.get(key).is_some() {
                coverage[key] = json!(0);
            }
        }
        for (available, omitted) in [
            ("prompts_available", "prompts_omitted_from_context"),
            ("runs_available", "runs_omitted_from_context"),
            (
                "spans_available_in_sampled_runs",
                "spans_omitted_from_context",
            ),
            (
                "events_available_in_sampled_runs",
                "events_omitted_from_context",
            ),
            ("available", "omitted_from_context"),
        ] {
            if let Some(count) = coverage[available].as_u64() {
                coverage[omitted] = json!(count);
            }
        }
        json!({"coverage":coverage,"input_omitted":true}).to_string()
    }
}

pub(crate) fn sync_evidence_coverage(value: &mut Value) {
    let prompts_included = value
        .get("user_prompts")
        .and_then(Value::as_array)
        .map(|items| items.len() as u64);
    let execution_counts = value
        .get("conversation_execution")
        .and_then(Value::as_object)
        .map(|execution| {
            ["runs", "spans", "events"].map(|key| {
                (
                    key,
                    execution
                        .get(key)
                        .and_then(Value::as_array)
                        .map(|items| items.len() as u64),
                )
            })
        })
        .unwrap_or_default();
    let captured_material_truncated = value["captured_material_truncated"] == true;
    let definition_omitted = value["coverage"]["definition_omitted_from_context"] == true;
    let Some(coverage) = value.get_mut("coverage").and_then(Value::as_object_mut) else {
        return;
    };
    let mut incomplete = false;
    let mut sync_count =
        |included: u64, included_key: &str, available_key: &str, omitted_key: &str| {
            if coverage.contains_key(included_key) {
                coverage.insert(included_key.into(), json!(included));
            }
            if let Some(available) = coverage.get(available_key).and_then(Value::as_u64) {
                let omitted = available.saturating_sub(included);
                if coverage.contains_key(omitted_key) || omitted > 0 {
                    coverage.insert(omitted_key.into(), json!(omitted));
                }
                incomplete |= omitted > 0;
            }
        };
    if let Some(included) = prompts_included {
        sync_count(
            included,
            "prompts_included",
            "prompts_available",
            "prompts_omitted_from_context",
        );
        sync_count(included, "included", "available", "omitted_from_context");
    }
    for ((_, included), (_, included_key, available_key, omitted_key)) in
        execution_counts.iter().zip([
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
        ])
    {
        if let Some(included) = included {
            sync_count(*included, included_key, available_key, omitted_key);
        }
    }
    if captured_material_truncated || definition_omitted {
        incomplete = true;
    }
    if incomplete {
        coverage.insert("complete".into(), Value::Bool(false));
        if coverage.contains_key("prompts_complete") {
            coverage.insert("prompts_complete".into(), Value::Bool(false));
        }
    }
}

fn increment_coverage(value: &mut Value, key: &str) {
    let current = value["coverage"][key].as_u64().unwrap_or(0);
    value["coverage"][key] = json!(current.saturating_add(1));
}

fn validate_screening_feedback(verdict: &Value, prompts: &[Value]) -> Result<Value, String> {
    let feedback = verdict["feedback"]
        .as_array()
        .ok_or("screener missing cited feedback array")?;
    let trigger = verdict["trigger"].as_bool().unwrap_or(false);
    for citation in feedback {
        let quote = citation["quote"]
            .as_str()
            .filter(|quote| !quote.trim().is_empty())
            .ok_or("screener feedback citation missing quote")?;
        let turn = citation["turn"]
            .as_i64()
            .ok_or("screener citation missing turn")?;
        if !prompts.iter().any(|prompt| {
            prompt["turn"].as_i64() == Some(turn)
                && prompt["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(quote))
        }) {
            return Err("screener feedback quote does not match a supplied user prompt".into());
        }
    }
    if trigger && feedback.is_empty() {
        return Err("triggered screener must cite a user prompt".into());
    }
    Ok(json!(feedback))
}

/// Commit the screening result and its derived jobs as one reset-fenced unit.
/// A reset that wins the database lock first invalidates this expected epoch;
/// a screen that commits first is then cancelled by the reset transaction.
pub(crate) fn commit_checkpoint_screening(
    portfolio: &Portfolio,
    checkpoint: &Checkpoint,
    outcome: &Value,
    trigger: bool,
    suggested_interval: i64,
    elapsed_interval_ms: u64,
) -> Result<bool, String> {
    let mut conn = portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let current_epoch: i64 = tx
        .query_row(
            "SELECT value FROM improvement_epoch WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if current_epoch != checkpoint.epoch {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(false);
    }
    let triggers: Value = serde_json::from_str(&checkpoint.triggers).unwrap_or(json!([]));
    let status = if trigger {
        "triggered"
    } else {
        "screened_no_trigger"
    };
    let completed = tx
        .execute(
            "UPDATE improvement_checkpoints SET status=?2,screener=?3,outcome=?3,updated_ms=?4
             WHERE id=?1 AND status='running' AND epoch=?5
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![
                checkpoint.id,
                status,
                outcome.to_string(),
                now_ms(),
                checkpoint.epoch
            ],
        )
        .map_err(|error| error.to_string())?;
    if completed != 1 {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(false);
    }
    let now = now_ms();
    tx.execute(
        "UPDATE improvement_conversation_state SET next_turn_at=?4,
         last_screened_turn=MAX(last_screened_turn,?5),
         next_elapsed_ms=CASE WHEN ?6 THEN ?7 ELSE next_elapsed_ms END,
         updated_ms=?8
         WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3 AND epoch=?9
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![
            checkpoint.conversation_id,
            checkpoint.workforce_id,
            checkpoint.owner_key_id,
            checkpoint.through_turn.saturating_add(suggested_interval),
            checkpoint.through_turn,
            triggers
                .as_array()
                .is_some_and(|items| items.iter().any(|item| item == "elapsed")),
            now.saturating_add(elapsed_interval_ms as i64),
            now,
            checkpoint.epoch
        ],
    )
    .map_err(|error| error.to_string())?;
    if trigger {
        tx.execute(
            "UPDATE improvement_jobs SET status='queued',checkpoint_id=?4,updated_ms=?5
             WHERE conversation_id=?1 AND entity_kind IN ('agent','workflow')
               AND status='pending_checkpoint' AND epoch=?7
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
               AND (workforce_id=?2 OR (workforce_id IS NULL AND ?2=''))
               AND (owner_key_id=?3 OR (owner_key_id IS NULL AND ?3=''))
               AND origin_run IN (SELECT run_id FROM improvement_turns
                 WHERE conversation_id=?1 AND workforce_id=?2 AND owner_key_id=?3
                   AND epoch=?7 AND status='released' ORDER BY id LIMIT ?6)",
            params![
                checkpoint.conversation_id,
                checkpoint.workforce_id,
                checkpoint.owner_key_id,
                checkpoint.id,
                now,
                checkpoint.through_turn,
                checkpoint.epoch
            ],
        )
        .map_err(|error| error.to_string())?;
        let auto_apply_private =
            auto_apply_private_eligible(&tx, &checkpoint.workforce_id, &checkpoint.owner_key_id)?;
        tx.execute(
            "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
              definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
              auto_apply,auto_apply_private,conversation_id,checkpoint_id,job_type)
              VALUES(?1,?2,?3,'consolidation','registry','{}','roster consolidation review','',
                ?9,'queued',?4,?4,?5,?6,0,?10,?7,?8,'consolidation')",
            params![
                uuid::Uuid::new_v4().to_string(),
                checkpoint.origin_request,
                checkpoint.origin_run,
                now,
                checkpoint.workforce_id,
                checkpoint.owner_key_id,
                checkpoint.conversation_id,
                checkpoint.id,
                checkpoint.epoch,
                auto_apply_private
            ],
        )
        .map_err(|error| error.to_string())?;
    }
    tx.commit().map_err(|error| error.to_string())?;
    let _ = portfolio.trace_bus.send(());
    Ok(true)
}

async fn process_checkpoint(state: &Arc<AppState>, checkpoint: Checkpoint) -> Result<(), String> {
    let execution_state = match scoped_state(
        state,
        (!checkpoint.workforce_id.is_empty()).then_some(checkpoint.workforce_id.as_str()),
        (!checkpoint.owner_key_id.is_empty()).then_some(checkpoint.owner_key_id.as_str()),
        false,
        &checkpoint.id,
    ) {
        Ok(Some(state)) => state,
        Ok(None) => {
            state.portfolio.conn.lock().unwrap().execute(
                "UPDATE improvement_checkpoints SET status='cancelled',outcome=?2,updated_ms=?3 WHERE id=?1",
                params![checkpoint.id,json!({"reason":"contributing key is revoked"}).to_string(),now_ms()],
            ).map_err(|error|error.to_string())?;
            return Ok(());
        }
        Err(error) => {
            state.portfolio.conn.lock().unwrap().execute(
                "UPDATE improvement_checkpoints SET status='cancelled',outcome=?2,updated_ms=?3 WHERE id=?1",
                params![checkpoint.id,json!({"reason":error}).to_string(),now_ms()],
            ).map_err(|e|e.to_string())?;
            return Ok(());
        }
    };
    let epoch: i64 = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT value FROM improvement_epoch WHERE id=1",
            [],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if epoch != checkpoint.epoch {
        state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE improvement_checkpoints SET status='cancelled',outcome=?2,updated_ms=?3
             WHERE id=?1 AND epoch=?4 AND status IN ('queued','running','triggered')",
                params![
                    checkpoint.id,
                    json!({"reason":"reset before checkpoint screening"}).to_string(),
                    now_ms(),
                    checkpoint.epoch
                ],
            )
            .map_err(|error| error.to_string())?;
        return Ok(());
    }
    let generation = state.portfolio.generation();
    let restored = match execution_state
        .portfolio
        .model_snapshot(&checkpoint.origin_run)?
    {
        Some(config) => crate::model_config::restore(&execution_state, config)?,
        None => execution_state.clone(),
    };
    let evidence = checkpoint_context(&restored.portfolio, &checkpoint)?;
    let request = format!("improvement-screen-{}", uuid::Uuid::new_v4());
    let trace_run = restored.portfolio.trace_begin(
        &request,
        &json!({"messages":[{"role":"user","content":"Prompt-only improvement feedback screening"}],"metadata":{"ccm_conversation_id":checkpoint.conversation_id,"improvement_checkpoint_id":checkpoint.id,"origin_request_id":checkpoint.origin_request}}),
        "improvement",
    )?;
    restored
        .portfolio
        .save_model_snapshot(&trace_run, &restored.config)?;
    let screener = crate::meta::feedback_screener(&restored, &request, generation).await?;
    let prompt_coverage = json!({
        "complete":evidence.coverage["prompts_complete"],
        "available":evidence.coverage["prompts_available"],
        "included":evidence.coverage["prompts_included"],
        "sampling":"first prompt plus most recent prompt samples"
    });
    let screening_input = bounded_json_input(
        json!({
            "user_prompts":evidence.prompts.clone(),
            "coverage":prompt_coverage,
            "configured_turn_bounds":{
                "minimum":restored.config.improvement.turns.minimum_interval,
                "maximum":restored.config.improvement.turns.maximum_interval
            }
        }),
        MAX_SCREENING_EVIDENCE_BYTES,
    );
    let response = crate::upstream::call(
        &restored,
        &request,
        "improvement_screener",
        crate::meta::FEEDBACK_SCREENER_ID,
        json!({"model":restored.config.model,"stream":false,"messages":[
            {"role":"system","content":format!("{}\n\nSCREENING CONTRACT\nRead only the supplied user prompt samples and prompt-coverage metadata. Do not use assistant output, tools, execution logs, definitions, or external facts. Trigger only for clear dissatisfaction/frustration or strong satisfaction that is useful feedback about the roster. Cite each triggered signal using an exact non-empty quote and the supplied 1-based conversation turn number. Return JSON {{\"trigger\":true|false,\"reason\":\"short signal or neutral\",\"feedback\":[{{\"turn\":1,\"quote\":\"exact substring\"}}],\"next_turn_interval\":integer}}. A trigger requires at least one valid citation. The next interval must be within configured bounds.",screener.system_prompt)},
            {"role":"user","content":screening_input}
        ]}),
    )
    .await;
    let result = async {
        let response = response?;
        let verdict = crate::upstream::object(&response)?;
        let trigger = verdict["trigger"].as_bool().ok_or("screener missing trigger boolean")?;
        let reason = verdict["reason"]
            .as_str()
            .filter(|reason| !reason.trim().is_empty())
            .ok_or("screener missing reason")?;
        let suggested = verdict["next_turn_interval"]
            .as_u64()
            .unwrap_or(restored.config.improvement.turns.initial_interval as u64)
            .clamp(
                restored.config.improvement.turns.minimum_interval as u64,
                restored.config.improvement.turns.maximum_interval as u64,
            ) as i64;
        let feedback = validate_screening_feedback(&verdict, &evidence.prompts)?;
        let triggers: Value = serde_json::from_str(&checkpoint.triggers).unwrap_or(json!([]));
        let outcome = json!({"trigger":trigger,"reason":reason,"feedback":feedback,"next_turn_interval":suggested,"triggers":triggers});
        commit_checkpoint_screening(
            &restored.portfolio,
            &checkpoint,
            &outcome,
            trigger,
            suggested,
            restored.config.improvement.elapsed.interval_ms,
        )?;
        restored.portfolio.trace_finish_request(&request,"completed",&outcome)?;
        Ok::<Value, String>(outcome)
    }
    .await;
    if let Err(error) = result {
        let _ = restored
            .portfolio
            .trace_finish_request(&request, "error", &json!({"error":error}));
        restored
            .portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE improvement_checkpoints SET status='failed',outcome=?2,updated_ms=?3
             WHERE id=?1 AND status='running' AND epoch=?4
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
                params![
                    checkpoint.id,
                    json!({"error":error}).to_string(),
                    now_ms(),
                    checkpoint.epoch
                ],
            )
            .map_err(|e| e.to_string())?;
        return Err(error);
    }
    let _ = state.portfolio.trace_bus.send(());
    Ok(())
}

/// One worker limits auto-improvement concurrency; no foreground caller awaits it.
#[cfg(test)]
pub async fn process_one(state: &Arc<AppState>) -> Result<bool, String> {
    process_one_claims(state, None).await
}

async fn process_one_admitted(
    state: &Arc<AppState>,
    admission_permit: crate::health::AdmissionPermit,
) -> Result<bool, String> {
    process_one_claims(state, Some(admission_permit)).await
}

async fn process_one_claims(
    state: &Arc<AppState>,
    _admission_permit: Option<crate::health::AdmissionPermit>,
) -> Result<bool, String> {
    // Elapsed scheduling is worker-driven so idle conversations are reviewed at
    // their deadline without requiring another foreground response. The lease
    // is acquired before scheduling/claiming and held through the complete
    // evaluator or checkpoint, since this work is not registered in AsyncSupervisor.
    let _ = enqueue_due_elapsed_checkpoint(state)?;
    if let Some(job) = claim(state, true)? {
        process_job(state, job).await?;
        return Ok(true);
    }
    if let Some(checkpoint) = claim_checkpoint(state)? {
        process_checkpoint(state, checkpoint).await?;
        return Ok(true);
    }
    if let Some(job) = claim(state, false)? {
        process_job(state, job).await?;
        return Ok(true);
    }
    Ok(false)
}

/// Recover a suggestion whose old split commit inserted the proposal but lost
/// the job receipt. The row itself is the idempotency key across upgrades.
fn recover_suggestion_receipt(state: &AppState, job: &Job) -> Result<bool, String> {
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let mut statement = tx
        .prepare(
            "SELECT id,status FROM improvement_suggestions WHERE job_id=?1
             AND workforce_id=?2 AND owner_key_id=?3
             ORDER BY created_ms,id",
        )
        .map_err(|error| error.to_string())?;
    let suggestions = statement
        .query_map(
            params![
                job.id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref())
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(statement);
    if suggestions.is_empty() {
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(false);
    }
    if job.auto_apply_private && suggestions.iter().any(|(_, status)| status == "pending") {
        // Keep the claimed job running so process_job can create its private
        // audit trace and attempt the idempotent atomic application. Re-running
        // the evaluator here would repeat a committed verdict unnecessarily.
        tx.commit().map_err(|error| error.to_string())?;
        return Ok(false);
    }
    let ids = suggestions
        .iter()
        .map(|(id, _)| id.as_str())
        .collect::<Vec<_>>();
    let applied = suggestions.iter().all(|(_, status)| status == "applied");
    let outcome = if ids.len() == 1 {
        json!({"suggestion_id":ids[0],"applied":applied,"recovered_receipt":true})
    } else {
        json!({"suggestion_ids":ids,"applied":applied,"recovered_receipt":true})
    };
    let status = if applied { "completed" } else { "suggested" };
    tx.execute(
        "UPDATE improvement_jobs SET status=?2,outcome=?3,updated_ms=?4
         WHERE id=?1 AND status='running' AND epoch=?5
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![job.id, status, outcome.to_string(), now_ms(), job.epoch],
    )
    .map_err(|error| error.to_string())?;
    tx.commit().map_err(|error| error.to_string())?;
    let _ = state.portfolio.trace_bus.send(());
    Ok(true)
}

fn pending_suggestions_for_job(state: &AppState, job: &Job) -> Result<Vec<String>, String> {
    let conn = state.portfolio.conn.lock().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT id FROM improvement_suggestions WHERE job_id=?1 AND workforce_id=?2
             AND owner_key_id=?3 AND status='pending' ORDER BY created_ms,id",
        )
        .map_err(|error| error.to_string())?;
    statement
        .query_map(
            params![
                job.id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref())
            ],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())
}

fn auto_private_authority(job: &Job) -> Result<ApplicationAuthority, String> {
    Ok(ApplicationAuthority::AutoPrivate {
        source_key_id: job
            .owner_key_id
            .clone()
            .ok_or("automatic private application has no source key")?,
        workforce_id: job
            .workforce_id
            .clone()
            .ok_or("automatic private application has no workforce")?,
        job_id: job.id.clone(),
    })
}

fn apply_private_suggestions(
    state: &Arc<AppState>,
    job: &Job,
    request: &str,
    mut outcome: Value,
) -> Result<Value, String> {
    if !job.auto_apply_private {
        return Ok(outcome);
    }
    let pending = pending_suggestions_for_job(state, job)?;
    if pending.is_empty() {
        return Ok(outcome);
    }
    let authority = auto_private_authority(job)?;
    let mut applied = Vec::new();
    let mut remaining = Vec::new();
    for (index, suggestion_id) in pending.iter().enumerate() {
        match apply_suggestion_with_authority(state, suggestion_id, request, &authority) {
            Ok(result) if result["applied"] == true || result["status"] == "applied" => {
                applied.push(suggestion_id.clone());
            }
            Ok(_) => remaining.push(suggestion_id.clone()),
            Err(error) if error == AUTO_PRIVATE_AUTHORIZATION_CHANGED => {
                remaining.extend(pending[index..].iter().cloned());
                break;
            }
            Err(error) => return Err(error),
        }
    }
    if !applied.is_empty() {
        outcome["auto_applied_suggestion_ids"] = json!(applied);
    }
    if !remaining.is_empty() {
        outcome["pending_suggestion_ids"] = json!(remaining);
        outcome["suggestion_id"] = outcome
            .get("suggestion_id")
            .cloned()
            .or_else(|| {
                outcome["pending_suggestion_ids"][0]
                    .as_str()
                    .map(|id| json!(id))
            })
            .unwrap_or(Value::Null);
        outcome["applied"] = Value::Bool(false);
    } else if !applied.is_empty() {
        outcome["applied"] = Value::Bool(true);
    }
    Ok(outcome)
}

fn persist_suggestion_receipt(
    portfolio: &Portfolio,
    job: &Job,
    action: &str,
    verdict: &Value,
) -> Result<Value, String> {
    let checkpoint_id = job
        .checkpoint_id
        .as_deref()
        .ok_or("suggestion job has no checkpoint")?;
    let conversation_id = job
        .conversation_id
        .as_deref()
        .ok_or("suggestion job has no conversation")?;
    let idempotency_key = format!("{}:{}:{}", job.id, job.kind, job.entity);
    let mut conn = portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let active: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_jobs WHERE id=?1 AND status='running'
             AND epoch=?2 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
             AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4))",
            params![job.id, job.epoch, job.workforce_id, job.owner_key_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if !active {
        return Err("background suggestion cancelled or reset before commit".into());
    }
    let existing: Option<(String, String)> = tx
        .query_row(
            "SELECT id,proposal FROM improvement_suggestions WHERE idempotency_key=?1
             OR (job_id=?2 AND workforce_id=?3 AND owner_key_id=?4
               AND action IN ('revise','retire')) ORDER BY created_ms,id LIMIT 1",
            params![
                idempotency_key,
                job.id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref())
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let (suggestion_id, persisted_verdict) = if let Some((existing, proposal)) = existing {
        (
            existing,
            serde_json::from_str::<Value>(&proposal).unwrap_or_else(|_| verdict.clone()),
        )
    } else {
        let suggestion_id = uuid::Uuid::new_v4().to_string();
        tx.execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,workforce_id,
             owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,created_ms,
             share_id,idempotency_key)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,'pending',?12,?13,?14)",
            params![
                suggestion_id,
                job.id,
                checkpoint_id,
                conversation_id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref()),
                job.kind,
                job.entity,
                action,
                verdict.to_string(),
                job.definition.to_string(),
                now_ms(),
                uuid::Uuid::new_v4().simple().to_string(),
                idempotency_key
            ],
        )
        .map_err(|error| error.to_string())?;
        (suggestion_id, verdict.clone())
    };
    let outcome = json!({
        "verdict":persisted_verdict,
        "suggestion_id":suggestion_id,
        "applied":false
    });
    let changed = tx
        .execute(
            "UPDATE improvement_jobs SET status='suggested',outcome=?2,updated_ms=?3
             WHERE id=?1 AND status='running' AND epoch=?4
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
               AND (?5 IS NULL OR workforce_id=?5) AND (?6 IS NULL OR owner_key_id=?6)",
            params![
                job.id,
                outcome.to_string(),
                now_ms(),
                job.epoch,
                job.workforce_id,
                job.owner_key_id
            ],
        )
        .map_err(|error| error.to_string())?;
    if changed != 1 {
        return Err("background suggestion receipt lost its job epoch".into());
    }
    tx.commit().map_err(|error| error.to_string())?;
    drop(conn);
    let _ = portfolio.trace_bus.send(());
    Ok(outcome)
}

#[derive(Clone)]
struct PendingCreateSuggestion {
    id: String,
    kind: String,
    requirements: String,
    plan_index: usize,
}

fn create_suggestion_values(verdict: &Value) -> Result<(String, String), String> {
    let object = verdict
        .as_object()
        .ok_or("create evaluation must be an object")?;
    if object.len() != 3
        || !object.contains_key("action")
        || !object.contains_key("kind")
        || !object.contains_key("requirements")
        || verdict["action"] != "create"
    {
        return Err("create evaluation must contain exactly action, kind, and requirements".into());
    }
    let kind = verdict["kind"]
        .as_str()
        .filter(|kind| ["agent", "workflow"].contains(kind))
        .ok_or("create evaluation has an invalid kind")?
        .to_owned();
    let requirements = verdict["requirements"]
        .as_str()
        .filter(|requirements| !requirements.trim().is_empty())
        .ok_or("create evaluation has empty requirements")?
        .to_owned();
    Ok((kind, requirements))
}

fn persist_create_suggestion_receipt(
    portfolio: &Portfolio,
    job: &Job,
    verdict: &Value,
) -> Result<(Vec<PendingCreateSuggestion>, Value), String> {
    let checkpoint_id = job
        .checkpoint_id
        .as_deref()
        .ok_or("create suggestion job has no checkpoint")?;
    let conversation_id = job
        .conversation_id
        .as_deref()
        .ok_or("create suggestion job has no conversation")?;
    let (kind, requirements) = create_suggestion_values(verdict)?;
    let plan_index = 0usize;
    let idempotency_key = format!("{}:create:{}:{}", job.id, kind, plan_index);
    let plan_item = json!({
        "action":"create",
        "kind":kind,
        "requirements":requirements
    });
    let plan_text = json!([plan_item]).to_string();
    let mut conn = portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    let active: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM improvement_jobs WHERE id=?1 AND status='running'
             AND epoch=?2 AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
             AND (?3 IS NULL OR workforce_id=?3) AND (?4 IS NULL OR owner_key_id=?4))",
            params![job.id, job.epoch, job.workforce_id, job.owner_key_id],
            |row| row.get(0),
        )
        .map_err(|error| error.to_string())?;
    if !active {
        return Err("background create suggestion cancelled or reset before commit".into());
    }
    let existing_plan: Option<String> = tx
        .query_row(
            "SELECT plan FROM workforce_plans WHERE job_id=?1
             AND (?2 IS NULL OR workforce_id=?2) AND (?3 IS NULL OR owner_key_id=?3)",
            params![job.id, job.workforce_id, job.owner_key_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    if let Some(existing) = existing_plan {
        let existing: Value = serde_json::from_str(&existing).map_err(|error| error.to_string())?;
        if existing != json!([plan_item]) {
            return Err("create suggestion conflicts with its durable workforce plan".into());
        }
    } else {
        tx.execute(
            "INSERT INTO workforce_plans(job_id,plan,workforce_id,owner_key_id)
             VALUES(?1,?2,?3,?4)",
            params![
                job.id,
                plan_text,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref())
            ],
        )
        .map_err(|error| error.to_string())?;
    }

    let existing: Option<(
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        String,
        Option<String>,
        String,
        String,
    )> = tx
        .query_row(
            "SELECT id,job_id,checkpoint_id,conversation_id,workforce_id,owner_key_id,
                    entity_kind,entity_id,action,proposal,share_id,before_definition,status
             FROM improvement_suggestions WHERE idempotency_key=?1",
            [&idempotency_key],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let suggestion_id = if let Some((
        id,
        existing_job,
        existing_checkpoint,
        existing_conversation,
        existing_workforce,
        existing_owner,
        existing_kind,
        existing_entity,
        existing_action,
        existing_proposal,
        existing_share_id,
        existing_before,
        existing_status,
    )) = existing
    {
        if existing_job != job.id
            || existing_checkpoint != checkpoint_id
            || existing_conversation != conversation_id
            || existing_workforce != scope_value(job.workforce_id.as_deref())
            || existing_owner != scope_value(job.owner_key_id.as_deref())
            || existing_kind != kind
            || !existing_entity.is_empty()
            || existing_action != "create"
            || existing_proposal != verdict.to_string()
            || existing_before != "null"
            || existing_share_id.as_deref().is_none_or(str::is_empty)
            || !matches!(existing_status.as_str(), "pending" | "applied")
        {
            return Err(
                "create suggestion idempotency key conflicts with its source receipt".into(),
            );
        }
        id
    } else {
        let id = uuid::Uuid::new_v4().to_string();
        tx.execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
             workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
             status,created_ms,share_id,idempotency_key)
             VALUES(?1,?2,?3,?4,?5,?6,?7,'','create',?8,'null','pending',?9,?10,?11)",
            params![
                id,
                job.id,
                checkpoint_id,
                conversation_id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref()),
                kind,
                verdict.to_string(),
                now_ms(),
                uuid::Uuid::new_v4().simple().to_string(),
                idempotency_key
            ],
        )
        .map_err(|error| error.to_string())?;
        id
    };
    let outcome = json!({
        "verdict":verdict,
        "suggestion_id":suggestion_id,
        "action":"create",
        "creation_suggestion":true,
        "applied":false
    });
    let changed = tx
        .execute(
            "UPDATE improvement_jobs SET status='suggested',outcome=?2,updated_ms=?3
             WHERE id=?1 AND status='running' AND epoch=?4
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
               AND (?5 IS NULL OR workforce_id=?5) AND (?6 IS NULL OR owner_key_id=?6)",
            params![
                job.id,
                outcome.to_string(),
                now_ms(),
                job.epoch,
                job.workforce_id,
                job.owner_key_id
            ],
        )
        .map_err(|error| error.to_string())?;
    if changed != 1 {
        return Err("background create suggestion receipt lost its job epoch".into());
    }
    tx.commit().map_err(|error| error.to_string())?;
    drop(conn);
    let _ = portfolio.trace_bus.send(());
    Ok((
        vec![PendingCreateSuggestion {
            id: suggestion_id,
            kind,
            requirements,
            plan_index,
        }],
        outcome,
    ))
}

fn pending_create_suggestions_for_job(
    state: &AppState,
    job: &Job,
) -> Result<Vec<PendingCreateSuggestion>, String> {
    let conn = state.portfolio.conn.lock().unwrap();
    let mut statement = conn
        .prepare(
            "SELECT id,entity_kind,proposal,idempotency_key FROM improvement_suggestions
             WHERE job_id=?1 AND workforce_id=?2 AND owner_key_id=?3
               AND action='create' AND status='pending' ORDER BY created_ms,id",
        )
        .map_err(|error| error.to_string())?;
    let rows = statement
        .query_map(
            params![
                job.id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref())
            ],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(statement);
    let plan_text: Option<String> = conn
        .query_row(
            "SELECT plan FROM workforce_plans WHERE job_id=?1
             AND workforce_id=?2 AND owner_key_id=?3",
            params![
                job.id,
                scope_value(job.workforce_id.as_deref()),
                scope_value(job.owner_key_id.as_deref())
            ],
            |row| row.get(0),
        )
        .optional()
        .map_err(|error| error.to_string())?;
    let Some(plan_text) = plan_text else {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        return Err("pending create suggestion has no durable workforce plan".into());
    };
    let plan: Value = serde_json::from_str(&plan_text).map_err(|error| error.to_string())?;
    let plan = plan
        .as_array()
        .ok_or("durable workforce plan is not an array")?;
    let mut pending = Vec::with_capacity(rows.len());
    for (id, entity_kind, proposal_text, idempotency_key) in rows {
        let proposal: Value =
            serde_json::from_str(&proposal_text).map_err(|error| error.to_string())?;
        let (kind, requirements) = create_suggestion_values(&proposal)?;
        if kind != entity_kind {
            return Err("create suggestion kind does not match its entity kind".into());
        }
        let prefix = format!("{}:create:{}:", job.id, kind);
        let index = idempotency_key
            .as_deref()
            .and_then(|key| key.strip_prefix(&prefix))
            .and_then(|index| index.parse::<usize>().ok())
            .ok_or("create suggestion has an invalid plan idempotency key")?;
        let plan_item = plan
            .get(index)
            .ok_or("create suggestion plan index is out of range")?;
        if *plan_item
            != json!({
                "action":"create",
                "kind":kind,
                "requirements":requirements
            })
        {
            return Err("create suggestion does not match its durable plan item".into());
        }
        pending.push(PendingCreateSuggestion {
            id,
            kind,
            requirements,
            plan_index: index,
        });
    }
    Ok(pending)
}

async fn apply_auto_private_creations(
    state: &Arc<AppState>,
    job: &Job,
    request: &str,
    suggestions: &[PendingCreateSuggestion],
    mut outcome: Value,
) -> Result<Value, String> {
    let authority = auto_private_authority(job)?;
    let mut created = Vec::new();
    let mut pending = Vec::new();
    for (index, suggestion) in suggestions.iter().enumerate() {
        match crate::registry::create_with_authority(
            state,
            request,
            &suggestion.kind,
            &suggestion.requirements,
            job.generation,
            Some((&job.id, suggestion.plan_index)),
            &authority,
        )
        .await
        {
            Ok(resource) => created.push(json!({
                "suggestion_id":suggestion.id,
                "kind":suggestion.kind,
                "id":resource["id"]
            })),
            Err(error) if error == AUTO_PRIVATE_AUTHORIZATION_CHANGED => {
                pending.extend(
                    suggestions[index..]
                        .iter()
                        .map(|suggestion| suggestion.id.clone()),
                );
                break;
            }
            Err(error) => return Err(error),
        }
    }
    outcome["creation_suggestion"] = Value::Bool(true);
    outcome["action"] = json!("create");
    outcome["auto_created"] = json!(created);
    if pending.is_empty() && !created.is_empty() {
        outcome["applied"] = Value::Bool(true);
    } else {
        if pending.is_empty() {
            pending.extend(suggestions.iter().map(|suggestion| suggestion.id.clone()));
        }
        outcome["pending_suggestion_ids"] = json!(pending);
        outcome["suggestion_id"] = outcome
            .get("suggestion_id")
            .cloned()
            .or_else(|| {
                outcome["pending_suggestion_ids"][0]
                    .as_str()
                    .map(|id| json!(id))
            })
            .unwrap_or(Value::Null);
        outcome["applied"] = Value::Bool(false);
    }
    Ok(outcome)
}

async fn process_job(state: &Arc<AppState>, job: Job) -> Result<(), String> {
    let is_policy = job.job_type == "policy" || job.kind == "workforce";
    let execution_state = match scoped_state(
        state,
        job.workforce_id.as_deref(),
        job.owner_key_id.as_deref(),
        is_policy,
        &job.id,
    ) {
        Ok(Some(state)) => state,
        Ok(None) => {
            finish_job_status(
                state,
                &job,
                "cancelled",
                json!({"reason":"contributing key is revoked"}),
            )?;
            return Ok(());
        }
        Err(error) => {
            let status = if is_policy { "failed" } else { "contributed" };
            finish_job_status(state, &job, status, json!({"reason":error}))?;
            return Ok(());
        }
    };
    if state.portfolio.generation() != job.generation {
        finish_job_status(
            state,
            &job,
            "cancelled",
            json!({"reason":"reset before evaluation"}),
        )?;
        return Ok(());
    }
    if !is_policy && recover_suggestion_receipt(state, &job)? {
        return Ok(());
    }
    let result = async {
        let restored = match execution_state.portfolio.model_snapshot(&job.origin_run)? {
            Some(config) => crate::model_config::restore(&execution_state, config)?,
            None => execution_state.clone(),
        };
        let mut metadata = json!({"improvement_job_id":job.id,"origin_run_id":job.origin_run});
        if let Some(conversation) = &job.conversation_id {
            metadata["ccm_conversation_id"] = json!(conversation);
        }
        let trace_run = restored.portfolio.trace_begin(
            &job.request,
            &json!({"messages":[{"role":"user","content":format!("Improvement review: {}",job.entity)}],"metadata":metadata}),
            "improvement",
        )?;
        restored.portfolio.save_model_snapshot(&trace_run, &restored.config)?;
        let updated = restored
            .portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE improvement_jobs SET evaluation_run=?2,updated_ms=?3
                 WHERE id=?1 AND status='running' AND epoch=?4
                   AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
                   AND (?5 IS NULL OR workforce_id=?5) AND (?6 IS NULL OR owner_key_id=?6)",
                params![job.id,trace_run,now_ms(),job.epoch,job.workforce_id,job.owner_key_id],
            )
            .map_err(|error| error.to_string())?;
        if updated != 1 {
            return Err("background operation cancelled or reset before evaluation".into());
        }

        if !is_policy && job.auto_apply_private {
            let pending_creations = pending_create_suggestions_for_job(&restored, &job)?;
            if !pending_creations.is_empty() {
                let suggestion_ids = pending_creations
                    .iter()
                    .map(|suggestion| suggestion.id.clone())
                    .collect::<Vec<_>>();
                let outcome = apply_auto_private_creations(
                    &restored,
                    &job,
                    &job.request,
                    &pending_creations,
                    json!({"recovered_suggestions":suggestion_ids}),
                )
                .await?;
                restored
                    .portfolio
                    .trace_finish_request(&job.request, "completed", &outcome)?;
                return Ok(outcome);
            }
            let pending = pending_suggestions_for_job(&restored, &job)?;
            if !pending.is_empty() {
                let outcome = apply_private_suggestions(
                    &restored,
                    &job,
                    &job.request,
                    json!({"recovered_suggestions":pending}),
                )?;
                restored
                    .portfolio
                    .trace_finish_request(&job.request, "completed", &outcome)?;
                return Ok(outcome);
            }
        }

        let outcome = if is_policy {
            crate::registry::improve_workforce(
                &restored,
                &job.request,
                &job.id,
                &job.definition,
                &job.task,
                &job.result,
                job.generation,
            )
            .await?
        } else if job.job_type == "consolidation" {
            let checkpoint_id = job
                .checkpoint_id
                .as_deref()
                .ok_or("consolidation review has no checkpoint")?;
            let checkpoint = load_checkpoint(&restored.portfolio, checkpoint_id)?
                .ok_or("consolidation checkpoint no longer exists")?;
            let evidence = checkpoint_context(&restored.portfolio, &checkpoint)?;
            let input = bounded_json_input(
                json!({
                    "user_prompts":evidence.prompts,
                    "conversation_execution":evidence.execution,
                    "coverage":evidence.coverage
                }),
                MAX_EVALUATION_EVIDENCE_BYTES,
            );
            crate::registry::consolidate(
                &restored,
                &job.request,
                &job.id,
                checkpoint_id,
                &checkpoint.conversation_id,
                &input,
                job.generation,
            )
            .await?
        } else {
            let checkpoint_id = job
                .checkpoint_id
                .as_deref()
                .ok_or("discretionary evaluation has no checkpoint")?;
            let checkpoint = load_checkpoint(&restored.portfolio, checkpoint_id)?
                .ok_or("improvement checkpoint no longer exists")?;
            let evidence = checkpoint_context(&restored.portfolio, &checkpoint)?;
            let prompt_coverage = json!({
                "complete":evidence.coverage["prompts_complete"],
                "available":evidence.coverage["prompts_available"],
                "included":evidence.coverage["prompts_included"],
                "sampling":"first prompt plus most recent prompt samples"
            });
            let task = bounded_json_input(
                json!({"user_prompts":evidence.prompts.clone(),"coverage":prompt_coverage}),
                MAX_SCREENING_EVIDENCE_BYTES,
            );
            let (captured_task, task_truncated) = truncate_utf8(&job.task, 4096);
            let (captured_result, result_truncated) = truncate_utf8(&job.result, 4096);
            let evidence_input = bounded_json_input(
                json!({
                    "conversation_execution":evidence.execution,
                    "coverage":evidence.coverage,
                    "captured_task":captured_task,
                    "captured_result":captured_result,
                    "captured_material_truncated":task_truncated || result_truncated
                }),
                MAX_EVALUATION_EVIDENCE_BYTES,
            );
            let verdict = crate::meta::evaluate_suggestion(
                &restored,
                &job.request,
                &job.kind,
                &job.entity,
                &job.definition,
                &task,
                &evidence_input,
                job.generation,
            )
            .await?;
            let action = verdict["action"].as_str().ok_or("evaluation missing action")?;
            if action == "keep" {
                json!({"verdict":verdict,"suggestion_id":Value::Null,"applied":false})
            } else if action == "create" {
                let (suggestions, outcome) = persist_create_suggestion_receipt(
                    &restored.portfolio,
                    &job,
                    &verdict,
                )?;
                if job.auto_apply_private {
                    apply_auto_private_creations(
                        &restored,
                        &job,
                        &job.request,
                        &suggestions,
                        outcome,
                    )
                    .await?
                } else {
                    outcome
                }
            } else {
                persist_suggestion_receipt(&restored.portfolio, &job, action, &verdict)?
            }
        };
        let outcome = if is_policy || outcome["creation_suggestion"] == true {
            outcome
        } else {
            apply_private_suggestions(&restored, &job, &job.request, outcome)?
        };
        restored.portfolio.trace_finish_request(&job.request,"completed",&outcome)?;
        Ok::<Value, String>(outcome)
    }
    .await;
    let (status, outcome) = match result {
        Ok(outcome) if outcome["applied"] == true => ("completed", outcome),
        Ok(outcome)
            if outcome["suggestion_id"].as_str().is_some()
                || outcome["suggestion_ids"]
                    .as_array()
                    .is_some_and(|ids| !ids.is_empty()) =>
        {
            ("suggested", outcome)
        }
        Ok(outcome) => ("completed", outcome),
        Err(error) if error.contains("provider admission timeout:") => {
            ("queued", json!({"waiting":error}))
        }
        Err(error) => ("failed", json!({"error":error})),
    };
    finish_job_status(state, &job, status, outcome)?;
    Ok(())
}

fn finish_job_status(
    state: &AppState,
    job: &Job,
    status: &str,
    outcome: Value,
) -> Result<(), String> {
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_jobs SET status=?2,outcome=?3,updated_ms=?4
             WHERE id=?1 AND status IN ('running','queued') AND epoch=?5
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
               AND (?6 IS NULL OR workforce_id=?6) AND (?7 IS NULL OR owner_key_id=?7)",
            params![
                job.id,
                status,
                outcome.to_string(),
                now_ms(),
                job.epoch,
                job.workforce_id,
                job.owner_key_id
            ],
        )
        .map_err(|error| error.to_string())?;
    let _ = state.portfolio.trace_bus.send(());
    Ok(())
}

fn load_checkpoint(portfolio: &Portfolio, id: &str) -> Result<Option<Checkpoint>, String> {
    let conn = portfolio.conn.lock().unwrap();
    conn.query_row(
        "SELECT id,conversation_id,origin_request,origin_run,through_turn,triggers,workforce_id,owner_key_id,epoch
         FROM improvement_checkpoints WHERE id=?1 AND workforce_id=?2 AND owner_key_id=?3
         AND status IN ('triggered','running') AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![id,scope_value(portfolio.workforce_scope()),scope_value(portfolio.key_scope())],
        |row| {
            Ok(Checkpoint {
                id: row.get(0)?,
                conversation_id: row.get(1)?,
                origin_request: row.get(2)?,
                origin_run: row.get(3)?,
                through_turn: row.get(4)?,
                triggers: row.get(5)?,
                workforce_id: row.get(6)?,
                owner_key_id: row.get(7)?,
                epoch: row.get(8)?,
            })
        },
    )
    .optional()
    .map_err(|error| error.to_string())
}

pub fn start(
    state: Arc<AppState>,
    lifecycle: Arc<crate::health::Lifecycle>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_worker(state, lifecycle))
}

async fn run_worker(state: Arc<AppState>, lifecycle: Arc<crate::health::Lifecycle>) {
    loop {
        // Improvement work is not registered as an AsyncSupervisor root. Admit
        // it directly through the shared lifecycle before elapsed scheduling or
        // any durable claim, then keep the lease through all provider calls,
        // receipts, mutations, and trace completion.
        let admission_permit = match lifecycle.try_admit_new_work() {
            Ok(permit) => permit,
            Err(crate::health::AdmissionError::Draining) => break,
        };
        match process_one_admitted(&state, admission_permit).await {
            Ok(true) => continue,
            Ok(false) => {}
            Err(error) => tracing::error!(%error,"improvement worker failed"),
        }
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_millis(250)) => {}
            _ = lifecycle.wait_until_draining() => break,
        }
    }
}

/// Mark work that was already claimed but has no durable final receipt as
/// interrupted/unknown. This is called only when the bounded shutdown drain had
/// to stop the worker. It deliberately does not use `cancelled` or `completed`,
/// and startup recovery only requeues rows left in `running`, so these rows are
/// not automatically replayed.
pub fn interrupt_in_flight_for_shutdown(state: &AppState) -> Result<usize, String> {
    let now = now_ms();
    let outcome = json!({
        "outcome":"unknown",
        "reason":"service shutdown before a durable improvement receipt"
    })
    .to_string();
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().map_err(|error| error.to_string())?;
    // Receipt-only reconciliation: a committed applied suggestion is durable even
    // if the worker was interrupted before writing the job status. Do not relabel
    // that receipt as unknown or replay it.
    let reconciled = tx
        .execute(
            "UPDATE improvement_jobs SET status='completed',outcome=?1,updated_ms=?2
             WHERE status='running'
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)
               AND EXISTS(SELECT 1 FROM improvement_suggestions s
                          WHERE s.job_id=improvement_jobs.id AND s.status='applied')
               AND NOT EXISTS(SELECT 1 FROM improvement_suggestions s
                              WHERE s.job_id=improvement_jobs.id AND s.status<>'applied')",
            params![
                json!({
                    "outcome":"completed",
                    "reconciled":"applied_suggestion_receipt",
                    "reason":"service shutdown observed a committed suggestion receipt"
                })
                .to_string(),
                now
            ],
        )
        .map_err(|error| error.to_string())?;
    let jobs = tx
        .execute(
            "UPDATE improvement_jobs SET status='interrupted',outcome=?1,updated_ms=?2
             WHERE status='running'
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
            params![outcome.clone(), now],
        )
        .map_err(|error| error.to_string())?;
    tx.execute(
        "UPDATE improvement_checkpoints SET status='interrupted',outcome=?1,updated_ms=?2
         WHERE status='running'
           AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)",
        params![outcome, now],
    )
    .map_err(|error| error.to_string())?;
    tx.commit().map_err(|error| error.to_string())?;
    Ok(jobs.saturating_add(reconciled))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::oneshot;

    #[test]
    fn retirement_suggestion_receipt_is_unique_when_a_running_job_is_requeued() {
        let portfolio = Portfolio::open(":memory:").unwrap();
        let now = now_ms();
        portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
                 definition,task,result,epoch,status,created_ms,updated_ms,conversation_id,
                 checkpoint_id,job_type)
                 VALUES('retire-job','retire-request','retire-origin-run','agent','retire-agent',
                 '{}','task','result',0,'running',?1,?1,'retire-conversation','retire-checkpoint','response')",
                [now],
            )
            .unwrap();
        let job = Job {
            id: "retire-job".into(),
            origin_run: "retire-origin-run".into(),
            kind: "agent".into(),
            entity: "retire-agent".into(),
            definition: json!({"id":"retire-agent"}),
            task: "task".into(),
            result: "result".into(),
            request: "retire-review-request".into(),
            generation: portfolio.generation(),
            epoch: 0,
            workforce_id: None,
            owner_key_id: None,
            conversation_id: Some("retire-conversation".into()),
            checkpoint_id: Some("retire-checkpoint".into()),
            job_type: "response".into(),
            auto_apply_private: false,
        };
        let first_verdict = json!({"action":"retire","reason":"no longer reusable"});
        let first = persist_suggestion_receipt(&portfolio, &job, "retire", &first_verdict).unwrap();
        assert_eq!(first["applied"], false);

        // Simulate a crash/requeue boundary after a legacy split receipt. The
        // suggestion row must remain the single durable outcome for this job.
        portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE improvement_jobs SET status='running',outcome=NULL WHERE id='retire-job'",
                [],
            )
            .unwrap();
        let second = persist_suggestion_receipt(
            &portfolio,
            &job,
            "retire",
            &json!({"action":"retire","reason":"replayed assessment"}),
        )
        .unwrap();
        assert_eq!(second["suggestion_id"], first["suggestion_id"]);
        assert_eq!(second["verdict"]["reason"], "no longer reusable");

        let (count, action, status, job_status): (i64, String, String, String) = portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT (SELECT COUNT(*) FROM improvement_suggestions WHERE job_id='retire-job'),
                 (SELECT action FROM improvement_suggestions WHERE job_id='retire-job'),
                 (SELECT status FROM improvement_suggestions WHERE job_id='retire-job'),
                 (SELECT status FROM improvement_jobs WHERE id='retire-job')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(action, "retire");
        assert_eq!(status, "pending");
        assert_eq!(job_status, "suggested");
    }

    #[test]
    fn create_verdict_persists_exact_plan_and_source_suggestion_idempotently() {
        let portfolio = Portfolio::open(":memory:").unwrap();
        let now = now_ms();
        portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
                 definition,task,result,epoch,status,created_ms,updated_ms,conversation_id,
                 checkpoint_id,job_type)
                 VALUES('create-job','create-origin','create-run','agent','evaluated-agent',
                 '{}','task','result',0,'running',?1,?1,'create-conversation',
                 'create-checkpoint','response')",
                [now],
            )
            .unwrap();
        let job = Job {
            id: "create-job".into(),
            origin_run: "create-run".into(),
            kind: "agent".into(),
            entity: "evaluated-agent".into(),
            definition: json!({}),
            task: "task".into(),
            result: "result".into(),
            request: "create-review".into(),
            generation: portfolio.generation(),
            epoch: 0,
            workforce_id: None,
            owner_key_id: None,
            conversation_id: Some("create-conversation".into()),
            checkpoint_id: Some("create-checkpoint".into()),
            job_type: "response".into(),
            auto_apply_private: false,
        };
        let verdict = json!({
            "action":"create",
            "kind":"workflow",
            "requirements":"A reusable workflow for the observed composition gap"
        });
        let (first_rows, first) =
            persist_create_suggestion_receipt(&portfolio, &job, &verdict).unwrap();
        assert_eq!(first["applied"], false);
        assert_eq!(first_rows.len(), 1);
        assert_eq!(first_rows[0].plan_index, 0);

        portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE improvement_jobs SET status='running',outcome=NULL WHERE id='create-job'",
                [],
            )
            .unwrap();
        let (replayed_rows, replayed) =
            persist_create_suggestion_receipt(&portfolio, &job, &verdict).unwrap();
        assert_eq!(replayed["suggestion_id"], first["suggestion_id"]);
        assert_eq!(replayed_rows[0].id, first_rows[0].id);

        let conn = portfolio.conn.lock().unwrap();
        let (plan_text, kind, entity_id, action, proposal, before, idempotency, count): (
            String,
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
        ) = conn
            .query_row(
                "SELECT (SELECT plan FROM workforce_plans WHERE job_id='create-job'),
                 entity_kind,entity_id,action,proposal,before_definition,idempotency_key,
                 (SELECT COUNT(*) FROM improvement_suggestions WHERE job_id='create-job')
                 FROM improvement_suggestions WHERE job_id='create-job'",
                [],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ))
                },
            )
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&plan_text).unwrap(),
            json!([verdict])
        );
        assert_eq!(kind, "workflow");
        assert_eq!(entity_id, "");
        assert_eq!(action, "create");
        assert_eq!(serde_json::from_str::<Value>(&proposal).unwrap(), verdict);
        assert_eq!(before, "null");
        assert_eq!(idempotency, "create-job:create:workflow:0");
        assert_eq!(count, 1);
    }

    #[test]
    fn consolidation_auto_apply_intent_requires_private_single_key_eligibility() {
        fn enqueue_consolidation(
            portfolio: &Portfolio,
            checkpoint_id: &str,
            auth: &crate::auth::AuthContext,
        ) -> bool {
            let epoch: i64 = portfolio
                .conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT value FROM improvement_epoch WHERE id=1",
                    [],
                    |row| row.get(0),
                )
                .unwrap();
            let now = now_ms();
            let conversation_id = format!("conversation-{checkpoint_id}");
            let origin_request = format!("request-{checkpoint_id}");
            let origin_run = format!("run-{checkpoint_id}");
            portfolio
                .conn
                .lock()
                .unwrap()
                .execute(
                    "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
                     origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms,epoch)
                     VALUES(?1,?2,?3,?4,?5,?6,1,'[\"turns\"]','running',?7,?7,?8)",
                    params![
                        checkpoint_id,
                        conversation_id,
                        auth.workforce_id,
                        auth.key_id,
                        origin_request,
                        origin_run,
                        now,
                        epoch
                    ],
                )
                .unwrap();
            let checkpoint = Checkpoint {
                id: checkpoint_id.into(),
                conversation_id,
                origin_request,
                origin_run,
                through_turn: 1,
                triggers: "[\"turns\"]".into(),
                workforce_id: auth.workforce_id.clone(),
                owner_key_id: auth.key_id.clone(),
                epoch,
            };
            assert!(
                commit_checkpoint_screening(
                    portfolio,
                    &checkpoint,
                    &json!({"trigger":true}),
                    true,
                    10,
                    3_600_000,
                )
                .unwrap()
            );
            portfolio
                .conn
                .lock()
                .unwrap()
                .query_row(
                    "SELECT auto_apply_private FROM improvement_jobs WHERE checkpoint_id=?1
                     AND job_type='consolidation'",
                    [checkpoint_id],
                    |row| row.get(0),
                )
                .unwrap()
        }

        let portfolio = Portfolio::open(":memory:").unwrap();
        let permissions = crate::auth::KeyPermissions {
            roster_use: true,
            roster_read: true,
            roster_write: true,
            improvement_contribute: true,
            bootstrap_admin: false,
        };
        let (single_key, _) = portfolio
            .create_key(
                "private single source",
                None,
                "private single",
                &permissions,
            )
            .unwrap();

        let shared_workforce = portfolio
            .create_workforce("explicitly shared", "shared")
            .unwrap();
        let (shared_source, _) = portfolio
            .create_key(
                "shared source",
                Some(&shared_workforce),
                "explicitly shared",
                &permissions,
            )
            .unwrap();

        let (private_multikey_source, _) = portfolio
            .create_key(
                "private multikey source",
                None,
                "private multikey",
                &permissions,
            )
            .unwrap();
        portfolio
            .create_key(
                "second private-workforce key",
                Some(&private_multikey_source.workforce_id),
                "private multikey",
                &permissions,
            )
            .unwrap();
        portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE workforces SET visibility='private' WHERE id=?1",
                [&private_multikey_source.workforce_id],
            )
            .unwrap();

        assert!(enqueue_consolidation(
            &portfolio,
            "single-private-checkpoint",
            &single_key
        ));
        assert!(!enqueue_consolidation(
            &portfolio,
            "shared-checkpoint",
            &shared_source
        ));
        assert!(!enqueue_consolidation(
            &portfolio,
            "private-multikey-checkpoint",
            &private_multikey_source
        ));
    }

    fn worker_test_state(upstream_base_url: &str) -> Arc<AppState> {
        Arc::new(AppState {
            providers: Arc::new(crate::provider_gate::Registry::new(
                &std::collections::BTreeMap::from([(
                    String::new(),
                    crate::model_config::Provider {
                        recovery: crate::provider_gate::Recovery {
                            cooldown_ms: 1,
                            max_cooldown_ms: 4,
                        },
                        ..Default::default()
                    },
                )]),
            )),
            config: Arc::new(crate::config::Config {
                routing: Default::default(),
                api_key: "worker-test".into(),
                upstream_base_url: upstream_base_url.into(),
                model: "worker-test-model".into(),
                host: "127.0.0.1".into(),
                port: 0,
                upstream: crate::config::UpstreamConfig {
                    first_response_timeout_ms: 10_000,
                    idle_timeout_ms: 10_000,
                    total_timeout_ms: 20_000,
                    max_attempts: 1,
                    ..Default::default()
                },
                portfolio_db: ":memory:".into(),
                max_iterations: 2,
                debug_enabled: false,
                personality_transparency: crate::config::PersonalityTransparency::default(),
                memory: crate::config::MemoryConfig::default(),
                shutdown: crate::config::ShutdownConfig::default(),
                creation: crate::config::CreationConfig::default(),
                improvement: ImprovementConfig::default(),
            }),
            http: reqwest::Client::new(),
            portfolio: Arc::new(Portfolio::open(":memory:").unwrap()),
            external_api_enabled: true,
            auth: None,
        })
    }

    fn seed_worker_job(
        state: &AppState,
        agent: &crate::portfolio::Agent,
        id: &str,
        checkpoint_status: &str,
    ) {
        let origin_request = format!("{id}-origin-request");
        let origin_run = state
            .portfolio
            .trace_begin(
                &origin_request,
                &json!({"messages":[{"role":"user","content":"worker lifecycle test"}]}),
                "external",
            )
            .unwrap();
        let checkpoint_id = format!("{id}-checkpoint");
        let conversation_id = format!("{id}-conversation");
        let now = now_ms();
        let epoch: i64 = state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT value FROM improvement_epoch WHERE id=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let conn = state.portfolio.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms,epoch)
             VALUES(?1,?2,'','',?3,?4,1,'[\"turns\"]',?5,?6,?6,?7)",
            params![
                checkpoint_id,
                conversation_id,
                origin_request,
                origin_run,
                checkpoint_status,
                now,
                epoch
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
             definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
             conversation_id,checkpoint_id,job_type)
             VALUES(?1,?2,?3,'agent',?4,?5,'captured task','captured result',?6,'queued',?7,?7,
             NULL,NULL,?8,?9,'response')",
            params![
                id,
                origin_request,
                origin_run,
                agent.id,
                json!(agent).to_string(),
                epoch,
                now,
                conversation_id,
                checkpoint_id
            ],
        )
        .unwrap();
    }

    struct ProviderBarrier {
        calls: AtomicUsize,
        entered: Mutex<Option<oneshot::Sender<()>>>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
    }

    async fn blocked_worker_provider(
        axum::extract::State(barrier): axum::extract::State<Arc<ProviderBarrier>>,
        axum::Json(_request): axum::Json<Value>,
    ) -> axum::Json<Value> {
        barrier.calls.fetch_add(1, Ordering::SeqCst);
        if let Some(entered) = barrier.entered.lock().unwrap().take() {
            let _ = entered.send(());
        }
        let release = barrier.release.lock().unwrap().take();
        if let Some(release) = release {
            let _ = release.await;
        }
        axum::Json(json!({"choices":[{"message":{"role":"assistant",
            "content":"{\"action\":\"keep\",\"reason\":\"still useful\",\
            \"evidence\":\"captured behavior is adequate\"}"},"finish_reason":"stop"}]}))
    }

    fn worker_test_agent(state: &AppState) -> crate::portfolio::Agent {
        state
            .portfolio
            .insert(&crate::portfolio::NewAgent {
                provider: String::new(),
                name: "worker lifecycle fixture".into(),
                purpose: "exercise admitted background evaluation".into(),
                system_prompt: "Return the requested JSON only.".into(),
                model: "worker-test-model".into(),
            })
            .unwrap()
    }

    #[tokio::test]
    async fn draining_before_worker_admission_leaves_jobs_and_checkpoints_queued() {
        let state = worker_test_state("http://127.0.0.1:1");
        let agent = worker_test_agent(&state);
        seed_worker_job(&state, &agent, "not-admitted-job", "queued");
        let lifecycle = Arc::new(crate::health::Lifecycle::new());
        lifecycle.mark_ready();
        lifecycle.begin_drain();

        run_worker(state.clone(), lifecycle.clone()).await;

        let (job_status, checkpoint_status): (String, String) = state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT j.status,c.status FROM improvement_jobs j
                 JOIN improvement_checkpoints c ON c.id=j.checkpoint_id WHERE j.id=?1",
                ["not-admitted-job"],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(job_status, "queued");
        assert_eq!(checkpoint_status, "queued");
        assert_eq!(lifecycle.active_admissions(), 0);
    }

    #[tokio::test]
    async fn drain_during_provider_call_keeps_permit_and_stops_before_next_claim() {
        let (entered_tx, entered_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let barrier = Arc::new(ProviderBarrier {
            calls: AtomicUsize::new(0),
            entered: Mutex::new(Some(entered_tx)),
            release: Mutex::new(Some(release_rx)),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let upstream_base_url = format!("http://{}", listener.local_addr().unwrap());
        let router = axum::Router::new()
            .route(
                "/chat/completions",
                axum::routing::post(blocked_worker_provider),
            )
            .with_state(barrier.clone());
        let server = tokio::spawn(async move {
            let _ = axum::serve(listener, router).await;
        });

        let state = worker_test_state(&upstream_base_url);
        let agent = worker_test_agent(&state);
        seed_worker_job(&state, &agent, "admitted-job", "triggered");
        seed_worker_job(&state, &agent, "after-drain-job", "triggered");
        let lifecycle = Arc::new(crate::health::Lifecycle::new());
        lifecycle.mark_ready();
        let worker = tokio::spawn(run_worker(state.clone(), lifecycle.clone()));

        tokio::time::timeout(std::time::Duration::from_secs(3), entered_rx)
            .await
            .expect("worker did not reach the provider")
            .expect("provider entry signal was dropped");
        assert_eq!(lifecycle.active_admissions(), 1);

        lifecycle.begin_drain();
        assert_eq!(lifecycle.active_admissions(), 1);
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(25),
                lifecycle.wait_for_admissions()
            )
            .await
            .is_err()
        );

        release_tx.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), worker)
            .await
            .expect("worker did not finish its admitted item and stop")
            .unwrap();

        let (completed_status, queued_status): (String, String) = state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT (SELECT status FROM improvement_jobs WHERE id='admitted-job'),
                 (SELECT status FROM improvement_jobs WHERE id='after-drain-job')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(completed_status, "completed");
        assert_eq!(queued_status, "queued");
        assert_eq!(barrier.calls.load(Ordering::SeqCst), 1);
        assert_eq!(lifecycle.active_admissions(), 0);

        server.abort();
    }
}
