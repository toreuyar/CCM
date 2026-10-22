//! Bearer-key authentication, workforce ownership, permissions, and bootstrap-only
//! key/workforce administration. Authentication state is deliberately reusable by
//! future HTTP/MCP adapters: handlers receive the same `AuthContext` extension.
use axum::{
    Json, Router,
    body::Body,
    extract::{Extension, Path, State},
    http::{Method, Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
    routing::get,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

const BOOTSTRAP_KEY_ID: &str = "bootstrap-admin-key";
const BOOTSTRAP_WORKFORCE_ID: &str = "bootstrap-admin-workforce";

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default)]
pub struct KeyPermissions {
    pub roster_use: bool,
    pub roster_read: bool,
    pub roster_write: bool,
    pub improvement_contribute: bool,
    pub bootstrap_admin: bool,
}

#[derive(Clone, Debug)]
pub struct AuthContext {
    pub key_id: String,
    pub workforce_id: String,
    pub permissions: KeyPermissions,
}

impl AuthContext {
    pub fn cache_partition(&self) -> String {
        format!(
            "{}:{}:{}:{}:{}:{}:{}",
            self.workforce_id,
            self.key_id,
            self.permissions.roster_use as u8,
            self.permissions.roster_read as u8,
            self.permissions.roster_write as u8,
            self.permissions.improvement_contribute as u8,
            self.permissions.bootstrap_admin as u8,
        )
    }

    pub fn allows(&self, permission: RequiredPermission) -> bool {
        match permission {
            RequiredPermission::Authenticated => true,
            RequiredPermission::RosterUse => self.permissions.roster_use,
            RequiredPermission::RosterRead => self.permissions.roster_read,
            RequiredPermission::ImprovementSuggestionsRead => {
                self.permissions.roster_read || self.permissions.improvement_contribute
            }
            RequiredPermission::RosterWrite => self.permissions.roster_write,
            RequiredPermission::ImprovementContribute => self.permissions.improvement_contribute,
            RequiredPermission::BootstrapAdmin => self.permissions.bootstrap_admin,
        }
    }

    pub fn require(&self, permission: RequiredPermission) -> Result<(), &'static str> {
        if self.allows(permission) {
            Ok(())
        } else {
            Err("bearer key lacks the required permission")
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum RequiredPermission {
    /// A valid bearer credential is sufficient for transport/protocol setup.
    /// MCP tool calls perform their own more specific permission checks.
    Authenticated,
    RosterUse,
    RosterRead,
    ImprovementSuggestionsRead,
    RosterWrite,
    ImprovementContribute,
    BootstrapAdmin,
}

#[derive(Clone, Copy)]
struct ScopedColumn {
    table: &'static str,
    columns: &'static [(&'static str, &'static str)],
}

const SCOPED_COLUMNS: &[ScopedColumn] = &[
    ScopedColumn {
        table: "agents",
        columns: &[("workforce_id", "TEXT")],
    },
    ScopedColumn {
        table: "workflows",
        columns: &[("workforce_id", "TEXT")],
    },
    ScopedColumn {
        table: "events",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "evaluations",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "workflow_runs",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "workforce_usage",
        columns: &[("workforce_id", "TEXT")],
    },
    ScopedColumn {
        table: "trace_runs",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "trace_spans",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "trace_events",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "trace_tools",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "trace_controls",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "trace_gates",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "definition_versions",
        columns: &[("workforce_id", "TEXT")],
    },
    ScopedColumn {
        table: "definition_changes",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "conversations",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "conversation_messages",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "improvement_jobs",
        columns: &[
            ("workforce_id", "TEXT"),
            ("owner_key_id", "TEXT"),
            ("auto_apply", "INTEGER NOT NULL DEFAULT 1"),
        ],
    },
    ScopedColumn {
        table: "improvement_responses",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "client_personality",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "client_creation_policy",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "request_creation_policy",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "agent_continuations",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "creation_receipts",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "workforce_plans",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "consolidation_aliases",
        columns: &[("workforce_id", "TEXT")],
    },
    ScopedColumn {
        table: "run_model_config",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
    ScopedColumn {
        table: "call_model_config",
        columns: &[("workforce_id", "TEXT"), ("owner_key_id", "TEXT")],
    },
];

/// Additive, restart-safe schema setup. Existing rows keep NULL ownership and are
/// deliberately not assigned to a workforce or key.
pub fn initialize(conn: &Connection) -> rusqlite::Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS ccm_schema_migrations(
            version INTEGER PRIMARY KEY, applied_ms INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS workforces(
            id TEXT PRIMARY KEY, name TEXT NOT NULL,
            visibility TEXT NOT NULL CHECK(visibility IN ('private','shared')),
            created_ms INTEGER NOT NULL);
         CREATE TABLE IF NOT EXISTS api_keys(
            id TEXT PRIMARY KEY, token_hash TEXT NOT NULL UNIQUE,
            workforce_id TEXT NOT NULL, label TEXT NOT NULL,
            roster_use INTEGER NOT NULL DEFAULT 0 CHECK(roster_use IN (0,1)),
            roster_read INTEGER NOT NULL DEFAULT 0 CHECK(roster_read IN (0,1)),
            roster_write INTEGER NOT NULL DEFAULT 0 CHECK(roster_write IN (0,1)),
            improvement_contribute INTEGER NOT NULL DEFAULT 0 CHECK(improvement_contribute IN (0,1)),
            bootstrap_admin INTEGER NOT NULL DEFAULT 0 CHECK(bootstrap_admin IN (0,1)),
            created_ms INTEGER NOT NULL, revoked_ms INTEGER);
         CREATE INDEX IF NOT EXISTS api_keys_workforce ON api_keys(workforce_id);
         CREATE INDEX IF NOT EXISTS api_keys_active_hash ON api_keys(token_hash,revoked_ms);",
    )?;

    let current: i64 = conn.query_row(
        "SELECT COALESCE(MAX(version),0) FROM ccm_schema_migrations",
        [],
        |row| row.get(0),
    )?;
    if current < 1 {
        let tx = conn.unchecked_transaction()?;
        for scoped in SCOPED_COLUMNS {
            for (column, kind) in scoped.columns {
                ensure_column(&tx, scoped.table, column, kind)?;
            }
        }
        tx.execute_batch(
            "CREATE INDEX IF NOT EXISTS agents_workforce ON agents(workforce_id);
             CREATE INDEX IF NOT EXISTS workflows_workforce ON workflows(workforce_id);
             CREATE INDEX IF NOT EXISTS workforce_usage_scope_time ON workforce_usage(workforce_id,entity_id,at_ms);
             CREATE INDEX IF NOT EXISTS trace_runs_owner_created ON trace_runs(workforce_id,owner_key_id,created_ms);
             CREATE INDEX IF NOT EXISTS trace_events_scope_seq ON trace_events(workforce_id,owner_key_id,seq);
             CREATE INDEX IF NOT EXISTS definition_versions_workforce ON definition_versions(workforce_id,entity_id);
             CREATE INDEX IF NOT EXISTS conversations_owner ON conversations(workforce_id,owner_key_id,created_ms);",
        )?;
        tx.execute(
            "INSERT OR IGNORE INTO ccm_schema_migrations(version,applied_ms) VALUES(1,?1)",
            [crate::observe::now_ms()],
        )?;
        tx.commit()?;
    }
    // Recreate these after ownership columns are present (the workforce schema
    // predates this migration and must also open cleanly on older databases).
    for table in ["agents", "workflows", "workforce_usage"] {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            conn.execute_batch(&format!(
                "DROP TRIGGER IF EXISTS workforce_{table}_{operation};"
            ))?;
            let owner = if operation == "DELETE" { "OLD" } else { "NEW" };
            conn.execute_batch(&format!(
                "CREATE TRIGGER workforce_{table}_{operation}
                 AFTER {operation} ON {table} BEGIN
                 INSERT INTO trace_events(run_id,kind,at_ms,data,workforce_id,owner_key_id)
                 VALUES('','workforce.changed',CAST(strftime('%s','now') AS INTEGER)*1000,'{{}}',
                 {owner}.workforce_id,NULL); END;"
            ))?;
        }
    }
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS improvement_created;
         DROP TRIGGER IF EXISTS improvement_updated;
         CREATE TRIGGER improvement_created AFTER INSERT ON improvement_jobs BEGIN
         INSERT INTO trace_events(run_id,kind,at_ms,data,workforce_id,owner_key_id)
         VALUES(NEW.origin_run,'improvement.changed',NEW.updated_ms,
                json_object('job_id',NEW.id,'status',NEW.status),NEW.workforce_id,NEW.owner_key_id); END;
         CREATE TRIGGER improvement_updated AFTER UPDATE ON improvement_jobs BEGIN
         INSERT INTO trace_events(run_id,kind,at_ms,data,workforce_id,owner_key_id)
         VALUES(NEW.origin_run,'improvement.changed',NEW.updated_ms,
                json_object('job_id',NEW.id,'status',NEW.status),NEW.workforce_id,NEW.owner_key_id); END;",
    )?;
    Ok(())
}

fn ensure_column(conn: &Connection, table: &str, column: &str, kind: &str) -> rusqlite::Result<()> {
    let existing = conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !existing.iter().any(|name| name == column) {
        conn.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"))?;
    }
    Ok(())
}

fn digest(token: &str) -> String {
    ring::digest::digest(&ring::digest::SHA256, token.as_bytes())
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn parse_bearer(headers: &axum::http::HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = value.split_once(' ')?;
    (scheme.eq_ignore_ascii_case("bearer")
        && !token.is_empty()
        && !token.bytes().any(|byte| byte.is_ascii_whitespace())
        && token.len() <= 512)
        .then_some(token)
}

async fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"error":"invalid or missing bearer key"})),
    )
        .into_response()
}

pub async fn middleware(
    State(state): State<Arc<crate::state::AppState>>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if !(path == "/api"
        || path.starts_with("/api/")
        || path == "/v1"
        || path.starts_with("/v1/")
        || path == "/mcp")
    {
        return next.run(request).await;
    }
    if !state.external_api_enabled {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"external APIs are disabled; configure CCM_BOOTSTRAP_ADMIN_KEY"})),
        )
            .into_response();
    }
    let Some(token) = parse_bearer(request.headers()) else {
        return unauthorized().await;
    };
    let context = match state.portfolio.authenticate_key(token) {
        Ok(Some(context)) => context,
        Ok(None) | Err(_) => return unauthorized().await,
    };
    let required = required_permission(request.method(), path);
    if !context.allows(required) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error":"bearer key lacks the required permission"})),
        )
            .into_response();
    }
    request.extensions_mut().insert(context);
    next.run(request).await
}

fn required_permission(method: &Method, path: &str) -> RequiredPermission {
    if path == "/mcp" {
        // MCP initialization and tool discovery require authentication; each
        // individual tool is checked against its own operation-specific scope.
        RequiredPermission::Authenticated
    } else if path == "/api/session" || path == "/api/runtime" {
        // Session capabilities are safe for any valid key; runtime applies its
        // own roster-read/bootstrap-admin projection before returning diagnostics.
        RequiredPermission::Authenticated
    } else if path == "/api/settings/overrides" {
        // Current-key preference controls have their own explicit permission;
        // roster_write never implies settings access.
        RequiredPermission::RosterUse
    } else if path.starts_with("/api/management/definitions/")
        && path.ends_with("/run")
        && method == Method::POST
    {
        RequiredPermission::RosterUse
    } else if path == "/api/admin" || path.starts_with("/api/admin/") {
        RequiredPermission::BootstrapAdmin
    } else if path == "/api/tool-approvals" || path.starts_with("/api/tool-approvals/") {
        // All approval operations are restricted to the current invoker's own
        // handles, so cancellation does not require general roster mutation.
        RequiredPermission::RosterUse
    } else if path == "/v1" || path.starts_with("/v1/") || path == "/api/chat" {
        RequiredPermission::RosterUse
    } else if path == "/api/improvements" && (method == Method::GET || method == Method::HEAD) {
        // Contributors retain access to their own private suggestions. The handler
        // separately gates other keys' sanitized projections on roster_read+write.
        RequiredPermission::ImprovementSuggestionsRead
    } else if method == Method::POST
        && path.starts_with("/api/improvements/")
        && path.ends_with("/declassify")
    {
        // Only the originating contributor may explicitly release generated
        // replacement text for shared-roster writers.
        RequiredPermission::ImprovementContribute
    } else if method == Method::GET || method == Method::HEAD {
        RequiredPermission::RosterRead
    } else {
        RequiredPermission::RosterWrite
    }
}

pub fn admin_routes() -> Router<Arc<crate::state::AppState>> {
    Router::new()
        .route(
            "/api/admin/workforces",
            get(list_workforces).post(create_workforce),
        )
        .route("/api/admin/keys", get(list_keys).post(create_key))
        .route("/api/admin/keys/{id}", axum::routing::delete(revoke_key))
}

#[derive(Deserialize)]
struct NewWorkforce {
    name: String,
    #[serde(default)]
    visibility: WorkforceVisibility,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum WorkforceVisibility {
    #[default]
    Private,
    Shared,
}

async fn list_workforces(
    State(state): State<Arc<crate::state::AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state
        .portfolio
        .list_workforces()
        .map(Json)
        .map_err(admin_error)
}

async fn create_workforce(
    State(state): State<Arc<crate::state::AppState>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Json(body): Json<NewWorkforce>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    if body.name.trim().is_empty() || body.name.len() > 120 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"name must contain 1 to 120 characters"})),
        ));
    }
    let _admission = admit_admin_mutation(lifecycle)?;
    state
        .portfolio
        .create_workforce(
            &body.name,
            match body.visibility {
                WorkforceVisibility::Private => "private",
                WorkforceVisibility::Shared => "shared",
            },
        )
        .map(|id| {
            (
                StatusCode::CREATED,
                Json(json!({"id":id,"name":body.name,"visibility":body.visibility})),
            )
        })
        .map_err(admin_error)
}

#[derive(Deserialize)]
struct NewKey {
    label: String,
    #[serde(default)]
    workforce_id: Option<String>,
    #[serde(default)]
    workforce_name: Option<String>,
    #[serde(default)]
    permissions: KeyPermissions,
}

async fn list_keys(
    State(state): State<Arc<crate::state::AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    state.portfolio.list_keys().map(Json).map_err(admin_error)
}

async fn create_key(
    State(state): State<Arc<crate::state::AppState>>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
    Json(body): Json<NewKey>,
) -> Result<(StatusCode, Json<Value>), (StatusCode, Json<Value>)> {
    if body.label.trim().is_empty() || body.label.len() > 120 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"label must contain 1 to 120 characters"})),
        ));
    }
    if body
        .workforce_name
        .as_ref()
        .is_some_and(|name| name.trim().is_empty() || name.len() > 120)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"workforce_name must contain 1 to 120 characters"})),
        ));
    }
    let _admission = admit_admin_mutation(lifecycle)?;
    state
        .portfolio
        .create_key(
            &body.label,
            body.workforce_id.as_deref(),
            body.workforce_name.as_deref().unwrap_or(&body.label),
            &body.permissions,
        )
        .map(|(context, token)| {
            (
                StatusCode::CREATED,
                Json(json!({"id":context.key_id,"workforce_id":context.workforce_id,"label":body.label,"token":token,"permissions":context.permissions,"shown_once":true})),
            )
        })
        .map_err(admin_error)
}

async fn revoke_key(
    State(state): State<Arc<crate::state::AppState>>,
    Path(id): Path<String>,
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
) -> Result<StatusCode, (StatusCode, Json<Value>)> {
    let _admission = admit_admin_mutation(lifecycle)?;
    state
        .portfolio
        .revoke_key(&id)
        .map(|()| StatusCode::NO_CONTENT)
        .map_err(admin_error)
}

fn admit_admin_mutation(
    lifecycle: Option<Extension<Arc<crate::health::Lifecycle>>>,
) -> Result<Option<crate::health::AdmissionPermit>, (StatusCode, Json<Value>)> {
    lifecycle
        .map(|Extension(lifecycle)| {
            lifecycle.try_admit_new_work().map_err(|error| {
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error":{
                        "message":error.to_string(),
                        "type":"service_unavailable",
                        "code":error.code(),
                        "retryable":error.is_retryable()
                    }})),
                )
            })
        })
        .transpose()
}

fn admin_error(error: String) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({"error":error})))
}

impl crate::portfolio::Portfolio {
    pub fn authenticate_key(&self, token: &str) -> rusqlite::Result<Option<AuthContext>> {
        let hash = digest(token);
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT id,workforce_id,roster_use,roster_read,roster_write,improvement_contribute,bootstrap_admin
                 FROM api_keys WHERE token_hash=?1 AND revoked_ms IS NULL",
                [hash],
                |row| {
                    Ok(AuthContext {
                        key_id: row.get(0)?,
                        workforce_id: row.get(1)?,
                        permissions: KeyPermissions {
                            roster_use: row.get(2)?,
                            roster_read: row.get(3)?,
                            roster_write: row.get(4)?,
                            improvement_contribute: row.get(5)?,
                            bootstrap_admin: row.get(6)?,
                        },
                    })
                },
            )
            .optional()
    }

    pub fn auth_context_for_key_id(&self, key_id: &str) -> rusqlite::Result<Option<AuthContext>> {
        self.conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT id,workforce_id,roster_use,roster_read,roster_write,improvement_contribute,bootstrap_admin
                 FROM api_keys WHERE id=?1 AND revoked_ms IS NULL",
                [key_id],
                |row| {
                    Ok(AuthContext {
                        key_id: row.get(0)?,
                        workforce_id: row.get(1)?,
                        permissions: KeyPermissions {
                            roster_use: row.get(2)?,
                            roster_read: row.get(3)?,
                            roster_write: row.get(4)?,
                            improvement_contribute: row.get(5)?,
                            bootstrap_admin: row.get(6)?,
                        },
                    })
                },
            )
            .optional()
    }

    /// Seed/rotate the only out-of-band bootstrap credential from
    /// `CCM_BOOTSTRAP_ADMIN_KEY`; it has no roster or read permissions.
    pub fn seed_bootstrap_key(&self, token: &str) -> rusqlite::Result<()> {
        let hash = digest(token);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO workforces(id,name,visibility,created_ms) VALUES(?1,'Bootstrap administration','private',?2)
             ON CONFLICT(id) DO NOTHING",
            params![BOOTSTRAP_WORKFORCE_ID, crate::observe::now_ms()],
        )?;
        conn.execute(
            "INSERT INTO api_keys(id,token_hash,workforce_id,label,bootstrap_admin,created_ms) VALUES(?1,?2,?3,'Bootstrap admin',1,?4)
             ON CONFLICT(id) DO UPDATE SET token_hash=excluded.token_hash,revoked_ms=NULL,bootstrap_admin=1",
            params![BOOTSTRAP_KEY_ID, hash, BOOTSTRAP_WORKFORCE_ID, crate::observe::now_ms()],
        )?;
        Ok(())
    }

    pub fn create_workforce(&self, name: &str, visibility: &str) -> Result<String, String> {
        if !["private", "shared"].contains(&visibility) {
            return Err("visibility must be private or shared".into());
        }
        let id = uuid::Uuid::new_v4().simple().to_string();
        self.conn
            .lock()
            .unwrap()
            .execute(
                "INSERT INTO workforces(id,name,visibility,created_ms) VALUES(?1,?2,?3,?4)",
                params![id, name, visibility, crate::observe::now_ms()],
            )
            .map_err(|error| error.to_string())?;
        Ok(id)
    }

    pub fn list_workforces(&self) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT w.id,w.name,w.visibility,w.created_ms,COUNT(k.id) FROM workforces w LEFT JOIN api_keys k ON k.workforce_id=w.id AND k.revoked_ms IS NULL GROUP BY w.id ORDER BY w.created_ms,w.id")
            .map_err(|error| error.to_string())?;
        let rows = stmt
            .query_map([], |row| {
                Ok(json!({"id":row.get::<_,String>(0)?,"name":row.get::<_,String>(1)?,"visibility":row.get::<_,String>(2)?,"created_ms":row.get::<_,i64>(3)?,"active_keys":row.get::<_,i64>(4)?}))
            })
            .map_err(|error| error.to_string())?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        Ok(json!(rows))
    }

    pub fn create_key(
        &self,
        label: &str,
        workforce_id: Option<&str>,
        workforce_name: &str,
        permissions: &KeyPermissions,
    ) -> Result<(AuthContext, String), String> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction().map_err(|error| error.to_string())?;
        let workforce = if let Some(workforce_id) = workforce_id {
            let exists: bool = tx
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM workforces WHERE id=?1)",
                    [workforce_id],
                    |row| row.get(0),
                )
                .map_err(|error| error.to_string())?;
            if !exists {
                return Err("unknown workforce".into());
            }
            tx.execute(
                "UPDATE workforces SET visibility='shared' WHERE id=?1",
                [workforce_id],
            )
            .map_err(|error| error.to_string())?;
            workforce_id.to_string()
        } else {
            let workforce = uuid::Uuid::new_v4().simple().to_string();
            tx.execute(
                "INSERT INTO workforces(id,name,visibility,created_ms) VALUES(?1,?2,'private',?3)",
                params![workforce, workforce_name, crate::observe::now_ms()],
            )
            .map_err(|error| error.to_string())?;
            workforce
        };
        let key_id = uuid::Uuid::new_v4().simple().to_string();
        let token = format!("ccm_{}", uuid::Uuid::new_v4().simple());
        tx.execute(
            "INSERT INTO api_keys(id,token_hash,workforce_id,label,roster_use,roster_read,roster_write,improvement_contribute,bootstrap_admin,created_ms)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![key_id,digest(&token),workforce,label,permissions.roster_use,permissions.roster_read,permissions.roster_write,permissions.improvement_contribute,permissions.bootstrap_admin,crate::observe::now_ms()],
        )
        .map_err(|error| error.to_string())?;
        tx.commit().map_err(|error| error.to_string())?;
        Ok((
            AuthContext {
                key_id,
                workforce_id: workforce,
                permissions: permissions.clone(),
            },
            token,
        ))
    }

    pub fn list_keys(&self) -> Result<Value, String> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id,workforce_id,label,roster_use,roster_read,roster_write,improvement_contribute,bootstrap_admin,created_ms,revoked_ms FROM api_keys ORDER BY created_ms,id",
        ).map_err(|error| error.to_string())?;
        let rows = stmt.query_map([], |row| {
            Ok(json!({"id":row.get::<_,String>(0)?,"workforce_id":row.get::<_,String>(1)?,"label":row.get::<_,String>(2)?,"permissions":{"roster_use":row.get::<_,bool>(3)?,"roster_read":row.get::<_,bool>(4)?,"roster_write":row.get::<_,bool>(5)?,"improvement_contribute":row.get::<_,bool>(6)?,"bootstrap_admin":row.get::<_,bool>(7)?},"created_ms":row.get::<_,i64>(8)?,"revoked_ms":row.get::<_,Option<i64>>(9)?}))
        }).map_err(|error| error.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|error| error.to_string())?;
        Ok(json!(rows))
    }

    pub fn revoke_key(&self, key_id: &str) -> Result<(), String> {
        if key_id == BOOTSTRAP_KEY_ID {
            return Err("the configured bootstrap key cannot be revoked through the API".into());
        }
        let changed = self
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE api_keys SET revoked_ms=COALESCE(revoked_ms,?2) WHERE id=?1",
                params![key_id, crate::observe::now_ms()],
            )
            .map_err(|error| error.to_string())?;
        if changed == 0 {
            return Err("unknown key".into());
        }
        Ok(())
    }
}
