//! Authentication, workforce isolation, and permission regression scenarios.
use super::*;
use futures::StreamExt;
use serde_json::Value;

fn permissions(
    roster_use: bool,
    roster_read: bool,
    roster_write: bool,
    improvement_contribute: bool,
    bootstrap_admin: bool,
) -> crate::auth::KeyPermissions {
    crate::auth::KeyPermissions {
        roster_use,
        roster_read,
        roster_write,
        improvement_contribute,
        bootstrap_admin,
    }
}

fn issue(
    state: &AppState,
    workforce: Option<&str>,
    permissions: crate::auth::KeyPermissions,
) -> (crate::auth::AuthContext, String) {
    state
        .portfolio
        .create_key("scenario key", workforce, "private workforce", &permissions)
        .unwrap()
}

fn bearer(client: &reqwest::Client, url: String, token: &str) -> reqwest::RequestBuilder {
    client.get(url).bearer_auth(token)
}

#[tokio::test]
async fn all_external_routes_reject_missing_unknown_and_revoked_keys() {
    let state = state("http://127.0.0.1:1", false);
    let (key, token) = issue(&state, None, permissions(true, true, false, false, false));
    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{}/v1/models", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{}/api/workforce", server.url))
            .bearer_auth("not-a-known-key")
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
    state.portfolio.revoke_key(&key.key_id).unwrap();
    assert_eq!(
        bearer(&client, format!("{}/v1/models", server.url), &token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn management_runtime_and_settings_routes_enforce_scoped_contracts() {
    let state = state("http://127.0.0.1:1", false);
    let (use_key, use_token) = issue(&state, None, permissions(true, false, false, false, false));
    let (_, read_token) = issue(&state, None, permissions(false, true, false, false, false));
    let (_, write_only_token) = issue(&state, None, permissions(false, false, true, false, false));
    let (_, admin_token) = issue(&state, None, permissions(false, false, false, false, true));
    let lifecycle = Arc::new(crate::health::Lifecycle::new());
    lifecycle.mark_ready();
    let app = crate::api::routes(state.clone()).layer(axum::Extension(lifecycle.clone()));
    let server = serve(app).await;
    let client = reqwest::Client::new();

    let session: Value = client
        .get(format!("{}/api/session", server.url))
        .bearer_auth(&use_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(session["identity"]["key_id"], use_key.key_id);
    assert!(
        session["supported_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| operation == "settings.overrides.write")
    );
    // A roster_use-only key cannot read /api/operations, so the session must not
    // advertise operations.read; the read-only key receives the truthful label.
    assert!(
        !session["supported_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| operation == "operations.read")
    );
    let reader_session: Value = client
        .get(format!("{}/api/session", server.url))
        .bearer_auth(&read_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        reader_session["supported_operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| operation == "operations.read")
    );
    assert_eq!(
        client
            .get(format!("{}/api/operations", server.url))
            .bearer_auth(&use_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let runtime: Value = client
        .get(format!("{}/api/runtime", server.url))
        .bearer_auth(&read_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(runtime["scope"]["type"], "key_workforce");
    assert!(runtime["providers"].is_null());
    assert_eq!(
        client
            .get(format!("{}/api/runtime", server.url))
            .bearer_auth(&use_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let settings_response = client
        .get(format!("{}/api/settings/overrides", server.url))
        .bearer_auth(&use_token)
        .send()
        .await
        .unwrap();
    assert_eq!(settings_response.status(), reqwest::StatusCode::OK);
    let initial_settings: Value = settings_response.json().await.unwrap();
    let initial_revision = initial_settings["revision"].as_str().unwrap().to_owned();
    assert_eq!(
        client
            .get(format!("{}/api/settings/overrides", server.url))
            .bearer_auth(&write_only_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN,
        "roster_write does not grant current-key settings access"
    );
    assert_eq!(
        client
            .get(format!("{}/api/admin/settings", server.url))
            .bearer_auth(&use_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let updated_response = client
        .put(format!("{}/api/settings/overrides", server.url))
        .bearer_auth(&use_token)
        .header("if-match", format!("\"{initial_revision}\""))
        .json(&json!({"changes":{"creation_policy":"Dynamic"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let updated: Value = updated_response.json().await.unwrap();
    assert_eq!(updated["saved"]["creation_policy"], "Dynamic");
    let updated_revision = updated["revision"].as_str().unwrap();
    assert_ne!(updated_revision, initial_revision);

    let conflict = client
        .put(format!("{}/api/settings/overrides", server.url))
        .bearer_auth(&use_token)
        .header("if-match", format!("\"{initial_revision}\""))
        .json(&json!({"changes":{"creation_policy":"AlwaysPostExecution"}}))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);

    assert_eq!(
        client
            .post(format!("{}/api/management/definitions", server.url))
            .bearer_auth(&use_token)
            .json(&json!({"kind":"agent","requirements":"must be denied"}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let denied_status = client
        .get(format!("{}/api/admin/service/status", server.url))
        .bearer_auth(&use_token)
        .send()
        .await
        .unwrap();
    assert_eq!(denied_status.status(), reqwest::StatusCode::FORBIDDEN);

    let service_status: Value = client
        .get(format!("{}/api/admin/service/status", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(service_status["phase"], "ready");
    assert_eq!(service_status["admission"]["active"], 0);
    assert_eq!(service_status["planned_restart"]["safe_to_stop"], false);
    assert!(
        service_status["planned_restart"]["blockers"]
            .as_array()
            .unwrap()
            .contains(&json!("service_not_draining"))
    );
    assert!(!service_status.to_string().contains("127.0.0.1:1"));

    let drain: Value = client
        .post(format!("{}/api/admin/service/drain", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(drain["accepted"], true);
    assert_eq!(lifecycle.phase(), "draining");
    let after_drain: Value = client
        .get(format!("{}/api/admin/service/status", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(after_drain["planned_restart"]["safe_to_stop"], true);

    let rejected = client
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(&use_token)
        .json(&json!({
            "model":"test-model",
            "messages":[{"role":"user","content":"must not start during drain"}]
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        rejected.json::<Value>().await.unwrap()["error"]["code"],
        "service_draining"
    );
}

#[tokio::test]
async fn external_routes_stay_closed_without_bootstrap_configuration() {
    let mut state = state("http://127.0.0.1:1", false);
    let (_, token) = issue(&state, None, permissions(true, true, true, false, false));
    Arc::get_mut(&mut state).unwrap().external_api_enabled = false;
    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();

    for path in ["/v1/models", "/api/workforce", "/api/admin/keys"] {
        let response = client
            .get(format!("{}{path}", server.url))
            .bearer_auth(&token)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    }
    assert_eq!(
        client
            .get(format!("{}/", server.url))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::OK
    );
}

#[tokio::test]
async fn admin_management_creates_private_defaults_and_explicit_shared_workforces() {
    let state = state("http://127.0.0.1:1", false);
    state
        .portfolio
        .seed_bootstrap_key("bootstrap-scenario-secret")
        .unwrap();
    let (_, ordinary) = issue(&state, None, permissions(true, true, false, false, false));
    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();

    let denied = client
        .get(format!("{}/api/admin/workforces", server.url))
        .bearer_auth(&ordinary)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);

    let first: Value = client
        .post(format!("{}/api/admin/keys", server.url))
        .bearer_auth("bootstrap-scenario-secret")
        .json(&json!({
            "label":"private",
            "permissions":{"roster_use":true,"roster_read":true}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(first["shown_once"].as_bool().unwrap());
    assert!(first["token"].as_str().unwrap().starts_with("ccm_"));

    let shared: Value = client
        .post(format!("{}/api/admin/keys", server.url))
        .bearer_auth("bootstrap-scenario-secret")
        .json(&json!({
            "label":"shared peer",
            "workforce_id":first["workforce_id"],
            "permissions":{"roster_use":true,"roster_read":true}
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shared["workforce_id"], first["workforce_id"]);
    assert_ne!(shared["id"], first["id"]);

    let workforces: Value = client
        .get(format!("{}/api/admin/workforces", server.url))
        .bearer_auth("bootstrap-scenario-secret")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let wf = workforces
        .as_array()
        .unwrap()
        .iter()
        .find(|wf| wf["id"] == first["workforce_id"])
        .unwrap();
    assert_eq!(wf["visibility"], "shared");
    assert_eq!(wf["active_keys"], 2);
    let listed = state.portfolio.list_keys().unwrap().to_string();
    assert!(!listed.contains(first["token"].as_str().unwrap()));
}

#[tokio::test]
async fn shared_roster_and_usage_are_visible_but_runs_and_sse_are_key_private() {
    let state = state("http://127.0.0.1:1", false);
    let (key_a, token_a) = issue(&state, None, permissions(true, true, true, true, false));
    let (key_b, token_b) = issue(
        &state,
        Some(&key_a.workforce_id),
        permissions(true, true, true, true, false),
    );
    let (_, token_other) = issue(&state, None, permissions(true, true, true, false, false));
    let agent = state
        .with_auth(&key_a)
        .portfolio
        .insert(&NewAgent {
            provider: String::new(),
            name: "shared agent".into(),
            purpose: "test sharing".into(),
            system_prompt: "shared".into(),
            model: "test-model".into(),
        })
        .unwrap();
    state
        .with_auth(&key_a)
        .portfolio
        .workforce_usage(&agent.id, "assignment", "shared-use", "private-run")
        .unwrap();

    let a_state = state.with_auth(&key_a);
    let run_a = a_state
        .portfolio
        .trace_begin(
            "private-request-a",
            &json!({"messages":[{"role":"user","content":"secret from key A"}]}),
            "external",
        )
        .unwrap();
    let cursor_b = state.with_auth(&key_b).portfolio.trace_cursor().unwrap();
    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();

    let workforce_a: Value = bearer(&client, format!("{}/api/workforce", server.url), &token_a)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let workforce_b: Value = bearer(&client, format!("{}/api/workforce", server.url), &token_b)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let workforce_other: Value = bearer(
        &client,
        format!("{}/api/workforce", server.url),
        &token_other,
    )
    .send()
    .await
    .unwrap()
    .json()
    .await
    .unwrap();
    assert_eq!(workforce_a["entities"].as_array().unwrap().len(), 1);
    assert_eq!(workforce_b["entities"].as_array().unwrap().len(), 1);
    assert_eq!(workforce_b["entities"][0]["usage"]["1h"]["assignment"], 1);
    assert!(workforce_other["entities"].as_array().unwrap().is_empty());

    let runs_b: Value = bearer(&client, format!("{}/api/runs", server.url), &token_b)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !runs_b["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["id"] == run_a)
    );
    let events_b: Value = client
        .get(format!(
            "{}/api/events/page?after=0&run={run_a}",
            server.url
        ))
        .bearer_auth(&token_b)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(events_b["events"].as_array().unwrap().is_empty());

    let sse = client
        .get(format!("{}/api/events?after={cursor_b}", server.url))
        .bearer_auth(&token_b)
        .send()
        .await
        .unwrap();
    let mut stream = sse.bytes_stream();
    a_state
        .portfolio
        .trace_emit(
            &run_a,
            None,
            "private.marker",
            &json!({"text":"secret from key A"}),
        )
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(250), stream.next())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn permissions_are_independent_and_controls_cannot_cross_keys() {
    let state = state("http://127.0.0.1:1", false);
    let (reader, reader_token) = issue(&state, None, permissions(false, true, false, false, false));
    let (_, use_token) = issue(
        &state,
        Some(&reader.workforce_id),
        permissions(true, false, false, false, false),
    );
    let (writer, writer_token) = issue(
        &state,
        Some(&reader.workforce_id),
        permissions(false, false, true, false, false),
    );
    let (peer_writer, peer_writer_token) = issue(
        &state,
        Some(&reader.workforce_id),
        permissions(false, false, true, false, false),
    );
    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();

    assert_eq!(
        bearer(&client, format!("{}/v1/models", server.url), &reader_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        bearer(&client, format!("{}/api/workforce", server.url), &use_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );
    assert_eq!(
        client
            .post(format!("{}/api/admin/workforces", server.url))
            .bearer_auth(&reader_token)
            .json(&json!({"name":"not allowed"}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let payload = json!({"messages":[{"role":"user","content":"control test"}]});
    let run = state
        .with_auth(&writer)
        .portfolio
        .trace_begin("writer-private-run", &payload, "external")
        .unwrap();
    let response = client
        .post(format!("{}/api/runs/{run}/control", server.url))
        .bearer_auth(&peer_writer_token)
        .json(&json!({"action":"interrupt","message":"not your run"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let response = client
        .post(format!("{}/api/runs/{run}/control", server.url))
        .bearer_auth(&writer_token)
        .json(&json!({"action":"interrupt","message":"owner control"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_ne!(writer.key_id, peer_writer.key_id);
}

#[tokio::test]
async fn tool_continuations_are_private_to_the_originating_key() {
    let state = state("http://127.0.0.1:1", false);
    let (owner, owner_token) = issue(&state, None, permissions(true, true, false, false, false));
    let (_, peer_token) = issue(
        &state,
        Some(&owner.workforce_id),
        permissions(true, true, false, false, false),
    );
    let uuid = uuid::Uuid::new_v4().simple().to_string();
    let tool_id = format!("ccm-agent:{uuid}:private-tool");
    let owner_state = state.with_auth(&owner);
    let run = owner_state
        .portfolio
        .trace_begin(
            "tool-owner-request",
            &json!({"messages":[{"role":"user","content":"request"}]}),
            "external",
        )
        .unwrap();
    owner_state
        .portfolio
        .trace_bind_tools(
            "tool-owner-request",
            &json!({"tool_calls":[{"id":tool_id,"type":"function","function":{"name":"private"}}]}),
        )
        .unwrap();

    let server = serve(crate::api::routes(state)).await;
    let response = reqwest::Client::new()
        .post(format!("{}/v1/chat/completions", server.url))
        .bearer_auth(peer_token)
        .json(&json!({"messages":[
            {"role":"assistant","tool_calls":[{"id":tool_id,"type":"function","function":{"name":"private"}}]},
            {"role":"tool","tool_call_id":tool_id,"content":"private result"}
        ]}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(!response.text().await.unwrap().contains("private result"));
    assert!(
        !owner_state
            .portfolio
            .trace_events(0, Some(&run))
            .unwrap()
            .is_empty()
    );
    // The owner token remains valid for the stored continuation.
    assert!(
        owner_state
            .portfolio
            .continuation_agent(&run)
            .unwrap()
            .is_none()
    );
    assert!(!owner_token.is_empty());
}

#[test]
fn additive_scope_migration_preserves_legacy_rows_as_unscoped() {
    let path = std::env::temp_dir().join(format!("ccm-auth-legacy-{}.db", uuid::Uuid::new_v4()));
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("CREATE TABLE agents(id TEXT PRIMARY KEY,name TEXT NOT NULL,purpose TEXT NOT NULL,system_prompt TEXT NOT NULL,model TEXT NOT NULL,created_at INTEGER NOT NULL,usage_count INTEGER NOT NULL DEFAULT 0,last_used_at INTEGER); INSERT INTO agents(id,name,purpose,system_prompt,model,created_at) VALUES('legacy','legacy','p','preserved','m',0);").unwrap();
    }
    let portfolio = Arc::new(Portfolio::open(path.to_str().unwrap()).unwrap());
    let (_, token) = portfolio
        .create_key(
            "new key",
            None,
            "new private workforce",
            &permissions(true, true, false, false, false),
        )
        .unwrap();
    let context = portfolio.authenticate_key(&token).unwrap().unwrap();
    assert!(portfolio.with_scope(&context).list().unwrap().is_empty());
    let legacy: (Option<String>, String) = portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT workforce_id,system_prompt FROM agents WHERE id='legacy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(legacy.0, None);
    assert_eq!(legacy.1, "preserved");
    drop(portfolio);
    std::fs::remove_file(&path).unwrap();
}

#[test]
fn contribution_permission_does_not_imply_shared_roster_write() {
    let state = state("http://127.0.0.1:1", false);
    let (contributor, _) = issue(&state, None, permissions(true, true, false, true, false));
    let (ordinary, _) = issue(
        &state,
        Some(&contributor.workforce_id),
        permissions(true, true, false, false, false),
    );
    let payload = json!({"messages":[{"role":"user","content":"task"}]});
    let contributor = state.with_auth(&contributor);
    let run = contributor
        .portfolio
        .trace_begin("contribution-request", &payload, "external")
        .unwrap();
    contributor
        .portfolio
        .queue_improvement(
            "contribution-request",
            "agent",
            "agent-id",
            &json!({"suggestion":"private evidence"}),
            "task",
            "result",
            0,
        )
        .unwrap();
    contributor
        .portfolio
        .release_improvements("contribution-request")
        .unwrap();
    assert_eq!(
        contributor.portfolio.improvements(&run).unwrap()[0]["status"],
        "pending_checkpoint"
    );

    let ordinary = state.with_auth(&ordinary);
    ordinary
        .portfolio
        .trace_begin("ordinary-request", &payload, "external")
        .unwrap();
    assert!(
        ordinary
            .portfolio
            .queue_improvement(
                "ordinary-request",
                "agent",
                "agent-id",
                &json!({}),
                "task",
                "result",
                0,
            )
            .is_err()
    );
}

#[tokio::test]
async fn shared_improvement_suggestions_are_allowlisted_and_apply_is_writer_private() {
    let state = state("http://127.0.0.1:1", false);
    let (contributor, contributor_token) =
        issue(&state, None, permissions(true, true, false, true, false));
    let (writer, writer_token) = issue(
        &state,
        Some(&contributor.workforce_id),
        permissions(true, true, true, false, false),
    );
    let (_, reader_token) = issue(
        &state,
        Some(&contributor.workforce_id),
        permissions(true, true, false, false, false),
    );
    let (_, writer_without_read_token) = issue(
        &state,
        Some(&contributor.workforce_id),
        permissions(true, false, true, true, false),
    );
    let (_, no_read_no_contribution_token) = issue(
        &state,
        Some(&contributor.workforce_id),
        permissions(true, false, false, false, false),
    );
    let (_, other_workforce_token) =
        issue(&state, None, permissions(true, true, true, false, false));
    let contributor_state = state.with_auth(&contributor);
    let agent = contributor_state
        .portfolio
        .insert(&NewAgent {
            provider: String::new(),
            name: "private suggestion target".into(),
            purpose: "test scoped application".into(),
            system_prompt: "Original scoped definition".into(),
            model: "test-model".into(),
        })
        .unwrap();
    let conversation = "private-source-conversation-id";
    let source_request = "private-source-request-id";
    let source_job = "private-source-job-id";
    let source_checkpoint = "private-source-checkpoint-id";
    let private_reason = "private-evaluator-reason-marker";
    let private_evidence = "private-evaluator-evidence-marker";
    let payload = json!({"messages":[{"role":"user","content":"Please improve this"}],
        "metadata":{"ccm_conversation_id":conversation}});
    let source_run = contributor_state
        .portfolio
        .trace_begin(source_request, &payload, "external")
        .unwrap();
    let suggestion_id = "private-source-suggestion-id";
    let share_id = "opaque-shared-suggestion-handle";
    let proposed_definition = json!({
        "provider":"",
        "id":agent.id,
        "name":"Proposed public name",
        "purpose":"Proposed public purpose",
        "system_prompt":"Proposed public instructions",
        "model":"test-model"
    });
    let evaluator_proposal = json!({
        "action":"revise",
        "reason":private_reason,
        "evidence":private_evidence,
        "replacement":{
            "provider":"",
            "name":"Proposed public name",
            "purpose":"Proposed public purpose",
            "system_prompt":"Proposed public instructions",
            "model":"test-model"
        }
    });
    contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
             workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
             status,created_ms,share_id) VALUES(?1,?2,?3,?4,?5,?6,'agent',?7,'revise',?8,?9,'pending',0,?10)",
            rusqlite::params![
                suggestion_id,
                source_job,
                source_checkpoint,
                conversation,
                contributor_state.portfolio.workforce_scope(),
                contributor_state.portfolio.key_scope(),
                agent.id,
                evaluator_proposal.to_string(),
                json!(agent).to_string(),
                share_id
            ],
        )
        .unwrap();

    // A write-enabled contributor without roster_read retains its own private
    // suggestion view, but not the other contributor's shared projection.
    let writer_without_read = state
        .portfolio
        .authenticate_key(&writer_without_read_token)
        .unwrap()
        .unwrap();
    state
        .with_auth(&writer_without_read)
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
             workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
             status,created_ms,share_id) VALUES('writer-own-private-id','writer-private-job',
             'writer-private-checkpoint','writer-private-conversation',?1,?2,'agent',?3,'revise',
             ?4,?5,'pending',0,'writer-private-share-handle')",
            rusqlite::params![
                writer_without_read.workforce_id,
                writer_without_read.key_id,
                agent.id,
                json!({"action":"revise","reason":"writer private reason",
                    "evidence":"writer private evidence","replacement":{}})
                .to_string(),
                json!(agent).to_string()
            ],
        )
        .unwrap();

    let server = serve(crate::api::routes(state)).await;
    let client = reqwest::Client::new();
    let own: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(own.as_array().unwrap().len(), 1);
    assert_eq!(own[0]["id"], suggestion_id);
    assert_eq!(own[0]["proposal"]["reason"], private_reason);

    let shared: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shared.as_array().unwrap().len(), 2);
    let shared_suggestion = shared
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == share_id)
        .unwrap();
    assert_eq!(shared_suggestion["id"], share_id);
    assert_eq!(shared_suggestion["kind"], "agent");
    assert_eq!(shared_suggestion["entity_id"], agent.id);
    assert_eq!(shared_suggestion["action"], "revise");
    assert_eq!(shared_suggestion["proposed_definition"], Value::Null);
    assert_eq!(shared_suggestion["declassification_required"], true);
    assert_eq!(
        shared_suggestion["note"],
        "based on private checkpoint evidence; details withheld"
    );
    assert_eq!(shared_suggestion["status"], "pending");
    let other_held_suggestion = shared
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "writer-private-share-handle")
        .unwrap();
    assert_eq!(other_held_suggestion["proposed_definition"], Value::Null);
    assert_eq!(other_held_suggestion["declassification_required"], true);
    let public_fields: std::collections::BTreeSet<_> = shared_suggestion
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        public_fields,
        [
            "id",
            "kind",
            "entity_id",
            "action",
            "proposed_definition",
            "declassification_required",
            "note",
            "status"
        ]
        .into_iter()
        .collect()
    );
    let shared_json = shared.to_string();
    for private in [
        suggestion_id,
        source_job,
        source_checkpoint,
        conversation,
        source_request,
        private_reason,
        private_evidence,
        contributor.key_id.as_str(),
        "Proposed public name",
        "Proposed public purpose",
        "Proposed public instructions",
        "writer-own-private-id",
        "writer-private-job",
        "writer-private-checkpoint",
        "writer-private-conversation",
        "writer private reason",
        "writer private evidence",
    ] {
        assert!(
            !shared_json.contains(private),
            "shared response leaked {private}"
        );
    }

    // Read-only keys keep their own private view but never receive another key's
    // shared projection. A writer without read permission cannot list proposals.
    let reader: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&reader_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(reader.as_array().unwrap().is_empty());
    let writer_without_read_items: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&writer_without_read_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(writer_without_read_items.as_array().unwrap().len(), 1);
    assert_eq!(writer_without_read_items[0]["id"], "writer-own-private-id");
    assert!(!writer_without_read_items.to_string().contains(share_id));
    let no_read = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&no_read_no_contribution_token)
        .send()
        .await
        .unwrap();
    assert_eq!(no_read.status(), reqwest::StatusCode::FORBIDDEN);

    let denied = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&reader_token)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::FORBIDDEN);

    let other_workforce: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&other_workforce_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(other_workforce.as_array().unwrap().is_empty());
    let cross_workforce_apply = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&other_workforce_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        cross_workforce_apply.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    let contributor_cannot_apply = client
        .post(format!(
            "{}/api/improvements/{suggestion_id}/apply",
            server.url
        ))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        contributor_cannot_apply.status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let reader_cannot_declassify = client
        .post(format!(
            "{}/api/improvements/{suggestion_id}/declassify",
            server.url
        ))
        .bearer_auth(&reader_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        reader_cannot_declassify.status(),
        reqwest::StatusCode::FORBIDDEN
    );
    let non_owner_cannot_declassify = client
        .post(format!(
            "{}/api/improvements/{suggestion_id}/declassify",
            server.url
        ))
        .bearer_auth(&writer_without_read_token)
        .send()
        .await
        .unwrap();
    assert_eq!(
        non_owner_cannot_declassify.status(),
        reqwest::StatusCode::NOT_FOUND
    );

    let pending_status: String = contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT status FROM improvement_suggestions WHERE id=?1",
            [suggestion_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending_status, "pending");

    let premature_apply = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(premature_apply.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        premature_apply
            .text()
            .await
            .unwrap()
            .contains("source-key declassification is required")
    );

    // Contribution permission alone can release the generated replacement; this
    // action does not apply it or mutate the shared definition.
    let declassified: Value = client
        .post(format!(
            "{}/api/improvements/{suggestion_id}/declassify",
            server.url
        ))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(declassified["id"], suggestion_id);
    assert_eq!(declassified["status"], "pending");
    assert_eq!(declassified["declassified"], true);
    let source_action: (Option<String>, i64) = contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT r.owner_key_id,COUNT(*) FROM trace_runs r
             JOIN trace_events e ON e.run_id=r.id
             WHERE e.kind='improvement.suggestion_declassified'
               AND json_extract(e.data,'$.suggestion_id')=?1
             GROUP BY r.owner_key_id",
            [suggestion_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(source_action, (Some(contributor.key_id.clone()), 1));

    let still_pending: (String, Option<i64>) = contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT status,applied_ms FROM improvement_suggestions WHERE id=?1",
            [suggestion_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(still_pending, ("pending".into(), None));
    assert_eq!(
        contributor_state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt,
        "Original scoped definition"
    );

    let released: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let released_suggestion = released
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == share_id)
        .unwrap();
    assert_eq!(
        released_suggestion["proposed_definition"],
        proposed_definition
    );
    assert_eq!(released_suggestion["declassification_required"], false);
    let other_held_after_release = released
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["id"] == "writer-private-share-handle")
        .unwrap();
    assert_eq!(other_held_after_release["proposed_definition"], Value::Null);
    assert_eq!(other_held_after_release["declassification_required"], true);
    let released_json = released.to_string();
    for private in [
        suggestion_id,
        source_job,
        source_checkpoint,
        conversation,
        source_request,
        private_reason,
        private_evidence,
        contributor.key_id.as_str(),
        "writer-own-private-id",
        "writer-private-job",
        "writer-private-checkpoint",
        "writer-private-conversation",
        "writer private reason",
        "writer private evidence",
    ] {
        assert!(
            !released_json.contains(private),
            "released shared projection leaked private source data {private}"
        );
    }
    let released_writer_view: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&writer_without_read_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(released_writer_view.as_array().unwrap().len(), 1);
    assert_eq!(released_writer_view[0]["id"], "writer-own-private-id");
    assert!(!released_writer_view.to_string().contains(share_id));
    assert!(
        !released_writer_view
            .to_string()
            .contains("Proposed public instructions")
    );

    let applied = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json::<Value>()
        .await
        .unwrap();
    assert_eq!(applied["status"], "applied");
    assert_eq!(applied["entity_id"], agent.id);
    assert!(applied["applied"].as_bool().unwrap());

    let own_after: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(own_after[0]["status"], "applied");
    assert!(own_after[0].get("application_request").is_none());
    assert!(!own_after.to_string().contains(&writer.key_id));

    let writer_lineage: Value = client
        .get(format!("{}/api/definitions/{}", server.url, agent.id))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(writer_lineage["changes"].as_array().unwrap().len(), 1);
    assert_eq!(writer_lineage["evaluations"].as_array().unwrap().len(), 1);
    assert_eq!(
        writer_lineage["changes"][0]["evidence"]["note"],
        "based on private checkpoint evidence; details withheld"
    );
    let writer_verdict: Value = serde_json::from_str(
        writer_lineage["evaluations"][0]["verdict"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        writer_verdict["reason"],
        "based on private checkpoint evidence; details withheld"
    );
    assert!(writer_verdict.get("evidence").is_none());
    let lineage_json = writer_lineage.to_string();
    for private in [
        source_job,
        source_checkpoint,
        conversation,
        source_request,
        private_reason,
        private_evidence,
        contributor.key_id.as_str(),
    ] {
        assert!(!lineage_json.contains(private), "lineage leaked {private}");
    }

    let contributor_lineage: Value = client
        .get(format!("{}/api/definitions/{}", server.url, agent.id))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        contributor_lineage["changes"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert!(
        contributor_lineage["evaluations"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let writer_run: String = contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT id FROM trace_runs WHERE source='improvement' AND owner_key_id=?1 ORDER BY created_ms DESC LIMIT 1",
            [&writer.key_id],
            |row| row.get(0),
        )
        .unwrap();
    let writer_conversation: Option<String> = contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT conversation_id FROM trace_runs WHERE id=?1",
            [&writer_run],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(writer_conversation, None);
    let writer_trace_data: Vec<String> = {
        let conn = contributor_state.portfolio.conn.lock().unwrap();
        let mut statement = conn
            .prepare("SELECT data FROM trace_events WHERE run_id=?1")
            .unwrap();
        statement
            .query_map([&writer_run], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    let writer_trace_json = writer_trace_data.join(" ");
    for private in [
        suggestion_id,
        source_job,
        source_checkpoint,
        conversation,
        source_request,
        private_reason,
        private_evidence,
        contributor.key_id.as_str(),
    ] {
        assert!(
            !writer_trace_json.contains(private),
            "trace leaked {private}"
        );
    }
    let source_run_access = client
        .get(format!("{}/api/runs/{source_run}", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap();
    assert_ne!(source_run_access.status(), reqwest::StatusCode::OK);
    let writer_runs: Value = client
        .get(format!("{}/api/runs", server.url))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !writer_runs["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["id"] == writer_run)
    );
    let contributor_writer_events: Value = client
        .get(format!(
            "{}/api/events/page?after=0&run={writer_run}",
            server.url
        ))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        contributor_writer_events["events"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let application_request: Option<String> = contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT application_request FROM improvement_suggestions WHERE id=?1",
            [suggestion_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(application_request, None);
}

#[tokio::test]
async fn shared_improvement_application_conflicts_when_definition_is_stale() {
    let state = state("http://127.0.0.1:1", false);
    let (contributor, contributor_token) =
        issue(&state, None, permissions(true, true, false, true, false));
    let (_, writer_token) = issue(
        &state,
        Some(&contributor.workforce_id),
        permissions(true, true, true, false, false),
    );
    let contributor_state = state.with_auth(&contributor);
    let agent = contributor_state
        .portfolio
        .insert(&NewAgent {
            provider: String::new(),
            name: "stale target".into(),
            purpose: "test stale shared suggestion".into(),
            system_prompt: "original definition".into(),
            model: "test-model".into(),
        })
        .unwrap();
    let suggestion_id = "stale-private-suggestion";
    let share_id = "stale-public-handle";
    contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
             workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
             status,created_ms,share_id) VALUES(?1,'private-job','private-checkpoint','private-conversation',
             ?2,?3,'agent',?4,'revise',?5,?6,'pending',0,?7)",
            rusqlite::params![
                suggestion_id,
                contributor_state.portfolio.workforce_scope(),
                contributor_state.portfolio.key_scope(),
                agent.id,
                json!({"action":"revise","reason":"private reason","evidence":"private evidence",
                    "replacement":{"provider":"","name":"revised","purpose":"revised purpose",
                        "system_prompt":"revised instructions","model":"test-model"}}).to_string(),
                json!(agent).to_string(),
                share_id
            ],
        )
        .unwrap();

    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();
    let released: Value = client
        .post(format!(
            "{}/api/improvements/{suggestion_id}/declassify",
            server.url
        ))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(released["declassified"], true);

    contributor_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE agents SET name='changed after evaluation' WHERE id=?1",
            [&agent.id],
        )
        .unwrap();

    let response = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CONFLICT);
    let own: Value = reqwest::Client::new()
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&contributor_token)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(own[0]["status"], "stale");
    let current = contributor_state.portfolio.get(&agent.id).unwrap().unwrap();
    assert_eq!(current.name, "changed after evaluation");
}

#[tokio::test]
async fn http_create_suggestion_apply_requires_release_and_replays_source_receipt() {
    let (provider, script) = upstream(vec![message(&definition().to_string())]).await;
    let base = state(&provider.url, false);
    let (writer, writer_token) = issue(&base, None, permissions(true, true, true, false, false));
    let (source, source_token) = issue(
        &base,
        Some(&writer.workforce_id),
        permissions(true, false, false, true, false),
    );
    let job_id = "http-create-apply-job";
    let suggestion_id = "http-private-create-suggestion";
    let share_id = "http-opaque-create-handle";
    let conversation_id = "http-private-create-conversation";
    let checkpoint_id = "http-private-create-checkpoint";
    let requirements = "HTTP_PRIVATE_CREATE_REQUIREMENTS_CANARY reusable writing agent";
    let proposal = json!({"action":"create","kind":"agent","requirements":requirements});
    let now = crate::observe::now_ms();
    let epoch: i64 = base
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
    {
        let conn = base.portfolio.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
             definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
             auto_apply_private,conversation_id,checkpoint_id,job_type)
             VALUES(?1,'http-create-origin-request','http-create-origin-run','agent','observed-agent',
             '{}','task','result',?2,'suggested',?3,?3,?4,?5,0,?6,?7,'response')",
            rusqlite::params![
                job_id,
                epoch,
                now,
                source.workforce_id,
                source.key_id,
                conversation_id,
                checkpoint_id
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO workforce_plans(job_id,plan,workforce_id,owner_key_id)
             VALUES(?1,?2,?3,?4)",
            rusqlite::params![
                job_id,
                json!([{"action":"create","kind":"agent","requirements":requirements}]).to_string(),
                source.workforce_id,
                source.key_id
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
             workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
             status,created_ms,share_id,idempotency_key)
             VALUES(?1,?2,?3,?4,?5,?6,'agent','','create',?7,'null','pending',?8,?9,?10)",
            rusqlite::params![
                suggestion_id,
                job_id,
                checkpoint_id,
                conversation_id,
                source.workforce_id,
                source.key_id,
                proposal.to_string(),
                now,
                share_id,
                format!("{job_id}:create:agent:0")
            ],
        )
        .unwrap();
    }

    let server = serve(crate::api::routes(base.clone())).await;
    let client = reqwest::Client::new();
    let shared_before: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shared_before[0]["id"], share_id);
    assert_eq!(shared_before[0]["requirements"], Value::Null);
    assert!(!shared_before.to_string().contains(requirements));

    let denied = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), reqwest::StatusCode::BAD_REQUEST);
    let denied_body: Value = denied.json().await.unwrap();
    assert_eq!(
        denied_body["error"],
        "source key release is required before cross-key creation apply"
    );
    for private in [
        requirements,
        suggestion_id,
        job_id,
        conversation_id,
        checkpoint_id,
    ] {
        assert!(!denied_body.to_string().contains(private));
    }
    assert!(script.lock().unwrap().1.is_empty());

    let released: Value = client
        .post(format!(
            "{}/api/improvements/{suggestion_id}/declassify",
            server.url
        ))
        .bearer_auth(&source_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(released["declassified"], true);
    assert!(!released.to_string().contains(requirements));

    let shared_after: Value = client
        .get(format!("{}/api/improvements", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(shared_after[0]["requirements"], requirements);
    assert_eq!(shared_after[0]["declassification_required"], false);

    let applied: Value = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let initial_fields = applied
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        initial_fields,
        [
            "id",
            "status",
            "action",
            "kind",
            "entity_id",
            "applied",
            "receipt"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(applied["id"], share_id);
    assert_eq!(applied["status"], "applied");
    assert_eq!(applied["action"], "create");
    assert_eq!(applied["applied"], true);
    assert_eq!(applied["receipt"], true);
    assert!(applied.get("suggestion_id").is_none());
    assert!(applied.get("job_status").is_none());
    let applied_text = applied.to_string();
    for private in [
        suggestion_id,
        job_id,
        conversation_id,
        checkpoint_id,
        "http-create-origin-request",
        "http-create-origin-run",
        source.key_id.as_str(),
    ] {
        assert!(
            !applied_text.contains(private),
            "create result leaked {private}"
        );
    }
    let entity_id = applied["entity_id"].as_str().unwrap();
    let provider_requests = script.lock().unwrap().1.clone();
    assert_eq!(provider_requests.len(), 1);
    let provider_request = provider_requests[0].to_string();
    assert!(provider_request.contains(requirements));
    assert!(!provider_request.contains(job_id));
    assert!(!provider_request.contains(conversation_id));
    assert!(!provider_request.contains(checkpoint_id));

    let replay: Value = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let replay_fields = replay
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        replay_fields,
        [
            "id",
            "status",
            "action",
            "kind",
            "entity_id",
            "applied",
            "receipt"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(replay["id"], share_id);
    assert_eq!(replay["receipt"], true);
    assert_eq!(replay["entity_id"], entity_id);
    assert!(replay.get("suggestion_id").is_none());
    assert!(replay.get("job_status").is_none());
    let replay_text = replay.to_string();
    for private in [
        suggestion_id,
        job_id,
        conversation_id,
        checkpoint_id,
        "http-create-origin-request",
        "http-create-origin-run",
        source.key_id.as_str(),
    ] {
        assert!(
            !replay_text.contains(private),
            "receipt replay leaked {private}"
        );
    }
    assert_eq!(script.lock().unwrap().1.len(), 1);

    // Simulate recovery after the source-owned creation receipt was committed
    // but before this pending suggestion recorded that receipt locally.
    {
        let conn = base.portfolio.conn.lock().unwrap();
        assert_eq!(
            conn.execute(
                "UPDATE improvement_suggestions SET status='pending',entity_id='',
                 applied_ms=NULL,application_request=NULL WHERE id=?1 AND status='applied'",
                [suggestion_id],
            )
            .unwrap(),
            1
        );
        assert_eq!(
            conn.execute(
                "UPDATE improvement_jobs SET status='suggested' WHERE id=?1 AND status='completed'",
                [job_id],
            )
            .unwrap(),
            1
        );
    }
    let recovered: Value = client
        .post(format!("{}/api/improvements/{share_id}/apply", server.url))
        .bearer_auth(&writer_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let recovered_fields = recovered
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(
        recovered_fields,
        [
            "id",
            "status",
            "action",
            "kind",
            "entity_id",
            "applied",
            "receipt"
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(recovered["id"], share_id);
    assert_eq!(recovered["receipt"], true);
    assert_eq!(recovered["entity_id"], entity_id);
    assert!(recovered.get("suggestion_id").is_none());
    assert!(recovered.get("job_status").is_none());
    let recovered_text = recovered.to_string();
    for private in [
        suggestion_id,
        job_id,
        conversation_id,
        checkpoint_id,
        "http-create-origin-request",
        "http-create-origin-run",
        source.key_id.as_str(),
    ] {
        assert!(
            !recovered_text.contains(private),
            "recovery replay leaked {private}"
        );
    }
    assert_eq!(script.lock().unwrap().1.len(), 1);

    let conn = base.portfolio.conn.lock().unwrap();
    let (receipt_owner, receipt_workforce, receipt_id, suggestion_status, job_status, count): (
        String,
        String,
        String,
        String,
        String,
        i64,
    ) = conn
        .query_row(
            "SELECT r.owner_key_id,r.workforce_id,r.entity_id,s.status,j.status,
             (SELECT COUNT(*) FROM creation_receipts WHERE job_id=?1)
             FROM creation_receipts r
             JOIN improvement_suggestions s ON s.job_id=r.job_id
             JOIN improvement_jobs j ON j.id=r.job_id
             WHERE r.job_id=?1 AND r.order_index=0 AND s.id=?2",
            rusqlite::params![job_id, suggestion_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(receipt_owner, source.key_id);
    assert_eq!(receipt_workforce, source.workforce_id);
    assert_eq!(receipt_id, entity_id);
    assert_eq!(suggestion_status, "applied");
    assert_eq!(job_status, "completed");
    assert_eq!(count, 1);
}

struct ConfigPathGuard(std::path::PathBuf);

impl Drop for ConfigPathGuard {
    fn drop(&mut self) {
        crate::management::set_test_config_path(None);
        let _ = std::fs::remove_file(&self.0);
    }
}

#[tokio::test]
async fn global_admin_settings_are_persisted_with_cas_and_never_granted_by_roster_write() {
    static CONFIG_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    let _serial = CONFIG_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await;

    let state = state("http://127.0.0.1:1", false);
    let (_, admin_token) = issue(&state, None, permissions(false, true, false, false, true));
    let (_, write_only_token) = issue(&state, None, permissions(false, false, true, false, false));
    let path = std::env::temp_dir().join(format!("ccm-settings-{}.json", uuid::Uuid::new_v4()));
    std::fs::write(
        &path,
        r#"{"providers":{"default":{"base_url":"http://127.0.0.1:1/v1","api_key_env":null}},"models":{"test-model":{}},"default_model":"test-model"}"#,
    )
    .unwrap();
    let initial_file = std::fs::read_to_string(&path).unwrap();
    let _guard = ConfigPathGuard(path.clone());
    crate::management::set_test_config_path(Some(path.clone()));

    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();

    assert_eq!(
        client
            .get(format!("{}/api/admin/settings", server.url))
            .bearer_auth(&write_only_token)
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN,
        "roster_write must not imply bootstrap administration"
    );
    assert_eq!(
        client
            .put(format!("{}/api/admin/settings", server.url))
            .bearer_auth(&write_only_token)
            .header("if-match", "\"anything\"")
            .json(&json!({"changes":{"memory":{"budget_mib":512}}}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::FORBIDDEN
    );

    let initial_response = client
        .get(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let initial_etag = initial_response.headers()["etag"]
        .to_str()
        .unwrap()
        .to_owned();
    let initial: Value = initial_response.json().await.unwrap();
    assert_eq!(initial["scope"], "global");
    assert_eq!(initial["saved"]["memory"]["budget_mib"], 256);
    let initial_revision = initial["revision"].as_str().unwrap().to_owned();
    assert_eq!(initial_etag, format!("\"{initial_revision}\""));

    let missing_precondition = client
        .put(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .json(&json!({"changes":{"memory":{"budget_mib":512}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        missing_precondition.status(),
        reqwest::StatusCode::PRECONDITION_REQUIRED
    );

    let stale = client
        .put(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .header("if-match", "\"stale-revision\"")
        .json(&json!({"changes":{"memory":{"budget_mib":512}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(stale.status(), reqwest::StatusCode::CONFLICT);
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        initial_file,
        "a conflicted compare-and-swap must not write the file"
    );

    let updated_response = client
        .put(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .header("if-match", format!("\"{initial_revision}\""))
        .json(&json!({"changes":{
            "personality_transparency":"extrovert",
            "upstream":{"idle_timeout_ms":45000},
            "memory":{"budget_mib":512}
        }}))
        .send()
        .await
        .unwrap();
    assert_eq!(updated_response.status(), reqwest::StatusCode::OK);
    let updated: Value = updated_response.json().await.unwrap();
    assert_eq!(updated["saved"]["personality_transparency"], "extrovert");
    assert_eq!(updated["saved"]["upstream"]["idle_timeout_ms"], 45000);
    assert_eq!(updated["saved"]["memory"]["budget_mib"], 512);
    // Saved changes are persisted but do not silently claim to be the frozen
    // process configuration before the documented restart boundary.
    assert_eq!(
        updated["effective"]["personality_transparency"],
        "introvert"
    );
    assert_eq!(updated["effective"]["memory"]["budget_mib"], 256);
    assert_eq!(
        updated["application_timing"]["personality_transparency"],
        "restart_required"
    );
    let updated_revision = updated["revision"].as_str().unwrap().to_owned();
    assert_ne!(updated_revision, initial_revision);
    let persisted: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(persisted["personality_transparency"], "extrovert");
    assert_eq!(persisted["upstream"]["idle_timeout_ms"], 45000);
    assert_eq!(persisted["memory"]["budget_mib"], 512);

    let readback: Value = client
        .get(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(readback["revision"], updated_revision);
    assert_eq!(readback["saved"]["upstream"]["idle_timeout_ms"], 45000);
    assert_eq!(readback["effective"]["upstream"]["idle_timeout_ms"], 400);

    let conflict = client
        .put(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .header("if-match", format!("\"{initial_revision}\""))
        .json(&json!({"changes":{"memory":{"budget_mib":1024}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(conflict.status(), reqwest::StatusCode::CONFLICT);

    let next = client
        .put(format!("{}/api/admin/settings", server.url))
        .bearer_auth(&admin_token)
        .header("if-match", format!("\"{updated_revision}\""))
        .json(&json!({"changes":{"memory":{"budget_mib":1024}}}))
        .send()
        .await
        .unwrap();
    assert_eq!(next.status(), reqwest::StatusCode::OK);
    let next: Value = next.json().await.unwrap();
    assert_eq!(next["saved"]["memory"]["budget_mib"], 1024);
    assert_eq!(next["effective"]["memory"]["budget_mib"], 256);
}

#[tokio::test]
async fn http_management_create_evaluate_run_and_retire_have_provider_owned_receipts_and_lineage() {
    let (upstream, script) = upstream(vec![
        message(&definition().to_string()),
        keep(),
        message("specialist execution report"),
        message("composed managed result"),
    ])
    .await;
    let state = state(&upstream.url, false);
    let (key, token) = issue(&state, None, permissions(true, true, true, false, false));
    let server = serve(crate::api::routes(state.clone())).await;
    let client = reqwest::Client::new();

    let created: Value = client
        .post(format!("{}/api/management/definitions", server.url))
        .bearer_auth(&token)
        .json(&json!({
            "kind":"agent",
            "requirements":"A reusable writer that returns an accurate report."
        }))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(created["status"], "created");
    assert_eq!(created["receipt"]["action"], "create");
    let id = created["resource"]["id"].as_str().unwrap().to_owned();
    assert!(
        state
            .portfolio
            .evaluations()
            .unwrap()
            .as_array()
            .unwrap()
            .is_empty(),
        "create-only management must not implicitly evaluate"
    );

    let evaluated: Value = client
        .post(format!(
            "{}/api/management/definitions/agent/{id}/evaluate",
            server.url
        ))
        .bearer_auth(&token)
        .json(&json!({"task":"draft report","result":"observed report"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(evaluated["status"], "evaluated");
    assert_eq!(evaluated["verdict"]["action"], "keep");
    assert_eq!(evaluated["applied"], false);
    assert_eq!(evaluated["receipt"]["action"], "keep");

    let run: Value = client
        .post(format!(
            "{}/api/management/definitions/agent/{id}/run",
            server.url
        ))
        .bearer_auth(&token)
        .json(&json!({"message":"draft the managed report"}))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(run["status"], "completed");
    assert_eq!(run["receipt"]["action"], "run");
    assert_eq!(
        run["result"]["assistant"]["content"],
        "composed managed result"
    );

    let retired: Value = client
        .post(format!(
            "{}/api/management/definitions/agent/{id}/retire",
            server.url
        ))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(retired["status"], "retired");
    assert_eq!(retired["receipt"]["action"], "retire");

    let (is_retired, evaluation_count, retire_changes, run_count): (bool, i64, i64, i64) = {
        let conn = state.portfolio.conn.lock().unwrap();
        let is_retired = conn
            .query_row("SELECT retired FROM agents WHERE id=?1", [&id], |row| {
                row.get(0)
            })
            .unwrap();
        let evaluation_count = conn
            .query_row(
                "SELECT COUNT(*) FROM evaluations WHERE entity_id=?1 AND owner_key_id=?2",
                rusqlite::params![&id, &key.key_id],
                |row| row.get(0),
            )
            .unwrap();
        let retire_changes = conn
            .query_row(
                "SELECT COUNT(*) FROM definition_changes WHERE entity_id=?1 AND action='retire'",
                [&id],
                |row| row.get(0),
            )
            .unwrap();
        let run_count = conn
            .query_row(
                "SELECT COUNT(*) FROM trace_requests q JOIN trace_runs r ON r.id=q.run_id
                 WHERE r.owner_key_id=?1 AND r.workforce_id=?2",
                rusqlite::params![&key.key_id, &key.workforce_id],
                |row| row.get(0),
            )
            .unwrap();
        (is_retired, evaluation_count, retire_changes, run_count)
    };
    assert!(is_retired);
    assert_eq!(evaluation_count, 1);
    assert_eq!(retire_changes, 1);
    assert!(
        run_count >= 4,
        "create, evaluate, run and retire are traced"
    );
    assert_eq!(script.lock().unwrap().1.len(), 4);
}

#[tokio::test]
async fn draining_service_rejects_http_mutations_before_effects_but_allows_existing_controls() {
    let state = state("http://127.0.0.1:1", false);
    let (_, admin_token) = issue(&state, None, permissions(false, true, true, true, true));
    let lifecycle = Arc::new(crate::health::Lifecycle::new());
    lifecycle.mark_ready();
    let server =
        serve(crate::api::routes(state.clone()).layer(axum::Extension(lifecycle.clone()))).await;
    let client = reqwest::Client::new();

    let initial: Value = client
        .get(format!("{}/api/admin/async-limits", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();

    lifecycle.begin_drain();
    let rejected_limit = client
        .put(format!("{}/api/admin/async-limits", server.url))
        .bearer_auth(&admin_token)
        .json(&json!({
            "max_root_operations":9,
            "max_child_workers":9,
            "max_queued_roots":9,
            "max_queue_wait_seconds":900
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        rejected_limit.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        rejected_limit.json::<Value>().await.unwrap()["error"]["code"],
        "service_draining"
    );
    assert_eq!(
        client
            .put(format!(
                "{}/api/admin/workforces/bootstrap-admin-workforce/async-limits",
                server.url
            ))
            .bearer_auth(&admin_token)
            .json(&json!({"max_root_operations":2}))
            .send()
            .await
            .unwrap()
            .status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    for (method, path) in [
        ("POST", "/api/improvements/unknown-suggestion/apply"),
        ("POST", "/api/improvements/unknown-suggestion/declassify"),
    ] {
        let request = if method == "POST" {
            client.post(format!("{}{}", server.url, path))
        } else {
            client.get(format!("{}{}", server.url, path))
        };
        let response = request.bearer_auth(&admin_token).send().await.unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "{path}"
        );
        assert_eq!(
            response.json::<Value>().await.unwrap()["error"]["code"],
            "service_draining"
        );
    }
    let reads: Value = client
        .get(format!("{}/api/admin/async-limits", server.url))
        .bearer_auth(&admin_token)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(reads["server"], initial["server"]);

    // Existing-run controls use the explicit existing-work admission path rather
    // than new-work admission. The unknown run still fails on lookup, proving the
    // request was not rejected merely because the service is draining.
    let control = client
        .post(format!("{}/api/runs/unknown-run/control", server.url))
        .bearer_auth(&admin_token)
        .json(&json!({"target":"foreman","action":"pause","message":"drain control"}))
        .send()
        .await
        .unwrap();
    assert_ne!(control.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(lifecycle.active_admissions(), 0);
}
