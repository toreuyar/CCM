//! Post-response ordering and durable auto-improvement recovery. Run when authorized.
use super::*;
use futures::StreamExt;

fn run_status(state: &AppState, run: &str) -> String {
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row("SELECT status FROM trace_runs WHERE id=?1", [run], |r| {
            r.get(0)
        })
        .unwrap()
}

fn set_improvement_config(
    state: &mut Arc<AppState>,
    improvement: crate::improvement::ImprovementConfig,
) {
    let mut config = (*state.config).clone();
    config.improvement = improvement;
    Arc::get_mut(state).unwrap().config = Arc::new(config);
}

fn record_turn(
    state: &Arc<AppState>,
    request: &str,
    conversation: &str,
    prompt: &str,
    agent: Option<&crate::portfolio::Agent>,
    usage_tokens: Option<i64>,
) -> String {
    let payload = json!({"messages":[{"role":"user","content":prompt}],
        "metadata":{"ccm_conversation_id":conversation}});
    let run = state
        .portfolio
        .trace_begin(request, &payload, "external")
        .unwrap();
    if let Some(tokens) = usage_tokens {
        let (_, span) = state.portfolio.trace_context(request).unwrap().unwrap();
        state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE trace_spans SET kind='agent_call',usage=?2 WHERE id=?1",
                rusqlite::params![span, json!({"total_tokens":tokens}).to_string()],
            )
            .unwrap();
    }
    if let Some(agent) = agent {
        state
            .portfolio
            .queue_improvement(
                request,
                "agent",
                &agent.id,
                &json!(agent),
                prompt,
                "captured report",
                state.portfolio.generation(),
            )
            .unwrap();
    }
    state
        .portfolio
        .capture_conversation_turn(request, &payload)
        .unwrap();
    state
        .portfolio
        .trace_finish_request(request, "completed", &json!({"ok":true}))
        .unwrap();
    state
        .portfolio
        .release_improvements_with_config(request, &state.config.improvement)
        .unwrap();
    run
}

fn turn_config(initial: u32, minimum: u32, maximum: u32) -> crate::improvement::ImprovementConfig {
    crate::improvement::ImprovementConfig {
        turns: crate::improvement::TurnCheckpointConfig {
            enabled: true,
            initial_interval: initial,
            minimum_interval: minimum,
            maximum_interval: maximum,
        },
        elapsed: crate::improvement::ElapsedCheckpointConfig {
            enabled: false,
            ..Default::default()
        },
        foreground_tokens: Default::default(),
    }
}

fn seed_private_revision_suggestion(
    state: &Arc<AppState>,
    auth: &crate::auth::AuthContext,
    agent: &crate::portfolio::Agent,
    auto_apply_private: bool,
) -> (String, String) {
    let job_id = uuid::Uuid::new_v4().to_string();
    let suggestion_id = uuid::Uuid::new_v4().to_string();
    let origin_request = format!("auto-private-origin-{}", &job_id[..8]);
    let payload = json!({"messages":[{"role":"user","content":"Improve the private writing specialist"}],
        "metadata":{"ccm_conversation_id":format!("conversation-{job_id}")}});
    let origin_run = state
        .portfolio
        .trace_begin(&origin_request, &payload, "external")
        .unwrap();
    let now = crate::observe::now_ms();
    let mut conn = state.portfolio.conn.lock().unwrap();
    let tx = conn.transaction().unwrap();
    let epoch: i64 = tx
        .query_row(
            "SELECT value FROM improvement_epoch WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    tx.execute(
        "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
          definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
          auto_apply_private,conversation_id,checkpoint_id,job_type)
         VALUES(?1,?2,?3,'agent',?4,?5,'captured task','captured report',?6,'suggested',?7,?7,
          ?8,?9,?10,?11,?12,'response')",
        rusqlite::params![
            job_id,
            origin_request,
            origin_run,
            agent.id,
            json!(agent).to_string(),
            epoch,
            now,
            auth.workforce_id,
            auth.key_id,
            auto_apply_private,
            format!("conversation-{job_id}"),
            format!("checkpoint-{job_id}")
        ],
    )
    .unwrap();
    let proposal = json!({
        "action":"revise",
        "reason":"private checkpoint evidence supports this reusable correction",
        "evidence":"the captured result violates the current output contract",
        "replacement":{
            "provider":agent.provider,
            "name":agent.name,
            "purpose":agent.purpose,
            "system_prompt":"Revised private writing specialist instructions",
            "model":agent.model
        }
    });
    tx.execute(
        "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
          workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
          status,created_ms,share_id,idempotency_key)
         VALUES(?1,?2,?3,?4,?5,?6,'agent',?7,'revise',?8,?9,'pending',?10,?11,?12)",
        rusqlite::params![
            suggestion_id,
            job_id,
            format!("checkpoint-{job_id}"),
            format!("conversation-{job_id}"),
            auth.workforce_id,
            auth.key_id,
            agent.id,
            proposal.to_string(),
            json!(agent).to_string(),
            now,
            uuid::Uuid::new_v4().simple().to_string(),
            format!("{job_id}:agent:{}", agent.id)
        ],
    )
    .unwrap();
    tx.commit().unwrap();
    (job_id, suggestion_id)
}

fn private_auto_authority(
    auth: &crate::auth::AuthContext,
    job_id: &str,
) -> crate::improvement::ApplicationAuthority {
    crate::improvement::ApplicationAuthority::AutoPrivate {
        source_key_id: auth.key_id.clone(),
        workforce_id: auth.workforce_id.clone(),
        job_id: job_id.to_string(),
    }
}

fn begin_auto_apply_request(state: &AppState, request: &str) {
    state
        .portfolio
        .trace_begin(
            request,
            &json!({"messages":[{"role":"user","content":"Apply my private improvement"}]}),
            "improvement",
        )
        .unwrap();
}

fn private_improvement_permissions() -> crate::auth::KeyPermissions {
    crate::auth::KeyPermissions {
        roster_use: true,
        roster_read: true,
        roster_write: true,
        improvement_contribute: true,
        bootstrap_admin: false,
    }
}

#[test]
fn private_auto_revision_commits_mutation_and_receipts_once() {
    let base = state("http://127.0.0.1:1", false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "private auto source",
            None,
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let (job_id, suggestion_id) = seed_private_revision_suggestion(&state, &auth, &agent, true);
    let authority = private_auto_authority(&auth, &job_id);
    let request = "private-auto-apply-success";
    begin_auto_apply_request(&state, request);

    let applied = crate::improvement::apply_suggestion_with_authority(
        &state,
        &suggestion_id,
        request,
        &authority,
    )
    .unwrap();
    assert_eq!(applied["status"], "applied");
    assert_eq!(applied["applied"], true);
    assert_eq!(
        state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt,
        "Revised private writing specialist instructions"
    );

    let replay = crate::improvement::apply_suggestion_with_authority(
        &state,
        &suggestion_id,
        request,
        &authority,
    )
    .unwrap();
    assert_eq!(replay["status"], "applied");
    let conn = state.portfolio.conn.lock().unwrap();
    let (suggestion_status, job_status, evaluations, changes): (String, String, i64, i64) = conn
        .query_row(
            "SELECT s.status,j.status,
             (SELECT COUNT(*) FROM evaluations WHERE request_id=?2),
             (SELECT COUNT(*) FROM definition_changes WHERE action='suggestion_revise'
               AND entity_id=?3)
             FROM improvement_suggestions s JOIN improvement_jobs j ON j.id=s.job_id
             WHERE s.id=?1",
            rusqlite::params![suggestion_id, request, agent.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(suggestion_status, "applied");
    assert_eq!(job_status, "completed");
    assert_eq!(evaluations, 1);
    assert_eq!(changes, 1);
}

#[test]
fn private_auto_revision_stays_pending_when_workforce_becomes_shared() {
    let base = state("http://127.0.0.1:1", false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "private auto source",
            None,
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let (job_id, suggestion_id) = seed_private_revision_suggestion(&state, &auth, &agent, true);
    let authority = private_auto_authority(&auth, &job_id);
    base.portfolio
        .create_key(
            "second workforce key",
            Some(&auth.workforce_id),
            "private auto workforce",
            &crate::auth::KeyPermissions::default(),
        )
        .unwrap();
    let request = "private-auto-apply-now-shared";
    begin_auto_apply_request(&state, request);

    let error = crate::improvement::apply_suggestion_with_authority(
        &state,
        &suggestion_id,
        request,
        &authority,
    )
    .unwrap_err();
    assert!(error.contains("no longer authorized"));
    assert_eq!(
        state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt,
        "Specialist instructions with scope evidence"
    );
    let conn = state.portfolio.conn.lock().unwrap();
    let (suggestion_status, job_status): (String, String) = conn
        .query_row(
            "SELECT s.status,j.status FROM improvement_suggestions s
             JOIN improvement_jobs j ON j.id=s.job_id WHERE s.id=?1",
            [&suggestion_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(suggestion_status, "pending");
    assert_eq!(job_status, "suggested");
}

#[test]
fn private_auto_revision_never_resolves_a_shared_handle() {
    let base = state("http://127.0.0.1:1", false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "private auto source",
            None,
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let (job_id, suggestion_id) = seed_private_revision_suggestion(&state, &auth, &agent, true);
    let authority = private_auto_authority(&auth, &job_id);
    let share_id: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT share_id FROM improvement_suggestions WHERE id=?1",
            [&suggestion_id],
            |row| row.get(0),
        )
        .unwrap();
    base.portfolio
        .create_key(
            "second workforce key",
            Some(&auth.workforce_id),
            "private auto workforce",
            &crate::auth::KeyPermissions::default(),
        )
        .unwrap();
    let request = "private-auto-no-share-fallback";
    begin_auto_apply_request(&state, request);

    let error =
        crate::improvement::apply_suggestion_with_authority(&state, &share_id, request, &authority)
            .unwrap_err();
    assert!(error.contains("no longer authorized"));
    let status: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT status FROM improvement_suggestions WHERE id=?1",
            [&suggestion_id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(status, "pending");
}

#[test]
fn private_auto_revision_rejects_a_reset_epoch_inside_the_mutation() {
    let base = state("http://127.0.0.1:1", false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "private auto source",
            None,
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let (job_id, suggestion_id) = seed_private_revision_suggestion(&state, &auth, &agent, true);
    let authority = private_auto_authority(&auth, &job_id);
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute("UPDATE improvement_epoch SET value=value+1 WHERE id=1", [])
        .unwrap();
    let request = "private-auto-apply-after-reset";
    begin_auto_apply_request(&state, request);

    let error = crate::improvement::apply_suggestion_with_authority(
        &state,
        &suggestion_id,
        request,
        &authority,
    )
    .unwrap_err();
    assert!(error.contains("no longer authorized"));
    let (suggestion_status, prompt): (String, String) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT s.status,a.system_prompt FROM improvement_suggestions s
             JOIN agents a ON a.id=s.entity_id WHERE s.id=?1",
            [&suggestion_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(suggestion_status, "pending");
    assert_eq!(prompt, "Specialist instructions with scope evidence");
}

#[test]
fn private_auto_revision_marks_a_stale_definition_without_applying() {
    let base = state("http://127.0.0.1:1", false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "private auto source",
            None,
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let (job_id, suggestion_id) = seed_private_revision_suggestion(&state, &auth, &agent, true);
    let authority = private_auto_authority(&auth, &job_id);
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE agents SET system_prompt='concurrent revision' WHERE id=?1",
            [&agent.id],
        )
        .unwrap();
    let request = "private-auto-apply-stale-snapshot";
    begin_auto_apply_request(&state, request);

    let error = crate::improvement::apply_suggestion_with_authority(
        &state,
        &suggestion_id,
        request,
        &authority,
    )
    .unwrap_err();
    assert!(error.contains("stale"));
    let (suggestion_status, prompt): (String, String) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT s.status,a.system_prompt FROM improvement_suggestions s
             JOIN agents a ON a.id=s.entity_id WHERE s.id=?1",
            [&suggestion_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(suggestion_status, "stale");
    assert_eq!(prompt, "concurrent revision");
}

#[test]
fn queue_marks_auto_private_intent_only_for_a_currently_eligible_workforce() {
    let base = state("http://127.0.0.1:1", false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "private auto source",
            None,
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let first_request = "auto-intent-private-capture";
    state
        .portfolio
        .trace_begin(
            first_request,
            &json!({"messages":[{"role":"user","content":"private"}]}),
            "external",
        )
        .unwrap();
    state
        .portfolio
        .queue_improvement(
            first_request,
            "agent",
            &agent.id,
            &json!(agent),
            "task",
            "result",
            state.portfolio.generation(),
        )
        .unwrap();
    let first_intent: bool = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT auto_apply_private FROM improvement_jobs WHERE origin_request=?1",
            [first_request],
            |row| row.get(0),
        )
        .unwrap();
    assert!(first_intent);

    base.portfolio
        .create_key(
            "second workforce key",
            Some(&auth.workforce_id),
            "private auto workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let second_request = "auto-intent-shared-capture";
    state
        .portfolio
        .trace_begin(
            second_request,
            &json!({"messages":[{"role":"user","content":"shared"}]}),
            "external",
        )
        .unwrap();
    state
        .portfolio
        .queue_improvement(
            second_request,
            "agent",
            &agent.id,
            &json!(agent),
            "task",
            "result",
            state.portfolio.generation(),
        )
        .unwrap();
    let second_intent: bool = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT auto_apply_private FROM improvement_jobs WHERE origin_request=?1",
            [second_request],
            |row| row.get(0),
        )
        .unwrap();
    assert!(!second_intent);
}

#[test]
fn released_create_suggestions_share_only_requirements() {
    let base = state("http://127.0.0.1:1", false);
    let (writer, _) = base
        .portfolio
        .create_key(
            "creation suggestion writer",
            None,
            "shared create suggestions",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: false,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let (source, _) = base
        .portfolio
        .create_key(
            "creation suggestion source",
            Some(&writer.workforce_id),
            "shared create suggestions",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: false,
                roster_write: false,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let writer_state = base.with_auth(&writer);
    let source_state = base.with_auth(&source);
    let suggestion_id = "create-requirements-release";
    let requirements = "private creation requirement marker";
    let proposal = json!({"action":"create","kind":"agent","requirements":requirements});
    source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
             workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
             status,created_ms,share_id,idempotency_key)
             VALUES(?1,'create-release-job','create-release-checkpoint','create-release-conversation',
             ?2,?3,'agent','','create',?4,'null','pending',?5,?6,'create-release-job:create:agent:0')",
            rusqlite::params![
                suggestion_id,
                source.workforce_id,
                source.key_id,
                proposal.to_string(),
                crate::observe::now_ms(),
                uuid::Uuid::new_v4().simple().to_string()
            ],
        )
        .unwrap();

    let private_view = source_state.portfolio.suggestions().unwrap();
    assert_eq!(private_view[0]["proposal"]["requirements"], requirements);
    let shared_before = writer_state.portfolio.suggestions().unwrap();
    let shared_before = &shared_before[0];
    assert_eq!(shared_before["action"], "create");
    assert_eq!(shared_before["requirements"], Value::Null);
    assert_eq!(shared_before["proposed_definition"], Value::Null);
    assert_eq!(shared_before["declassification_required"], true);
    assert!(!shared_before.to_string().contains(requirements));

    let release_request = "release-create-requirements";
    source_state
        .portfolio
        .trace_begin(
            release_request,
            &json!({"messages":[{"role":"user","content":"Release these creation requirements"}]}),
            "improvement",
        )
        .unwrap();
    crate::improvement::declassify_suggestion(&source_state, suggestion_id, release_request)
        .unwrap();
    let shared_after = writer_state.portfolio.suggestions().unwrap();
    let shared_after = &shared_after[0];
    assert_eq!(shared_after["requirements"], requirements);
    assert_eq!(shared_after["proposed_definition"], Value::Null);
    assert_eq!(shared_after["declassification_required"], false);
    assert!(!shared_after.to_string().contains("private reason"));
}

#[tokio::test]
async fn explicit_create_apply_waits_for_source_release_and_uses_source_receipt_owner() {
    let (provider, script) = upstream(vec![message(&definition().to_string())]).await;
    let base = state(&provider.url, false);
    let (writer, _) = base
        .portfolio
        .create_key(
            "create apply writer",
            None,
            "create apply workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: false,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let (source, _) = base
        .portfolio
        .create_key(
            "create apply source",
            Some(&writer.workforce_id),
            "create apply workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: false,
                roster_write: false,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let writer_state = base.with_auth(&writer);
    let source_state = base.with_auth(&source);
    let job_id = "explicit-create-job";
    let suggestion_id = "explicit-create-suggestion";
    let share_id = "explicit-create-handle";
    let conversation_id = "explicit-create-conversation";
    let checkpoint_id = "explicit-create-checkpoint";
    let requirements = "Create a reusable agent for the observed writing gap";
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
             VALUES(?1,'create-origin','create-run','agent','observed-agent','{}','task','result',
             ?2,'suggested',?3,?3,?4,?5,0,?6,?7,'response')",
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

    let blocked_request = "create-apply-before-release";
    writer_state
        .portfolio
        .trace_begin(
            blocked_request,
            &json!({"messages":[{"role":"user","content":"Apply this create proposal"}]}),
            "improvement",
        )
        .unwrap();
    let blocked = crate::improvement::apply_suggestion_async_with_authority(
        &writer_state,
        share_id,
        blocked_request,
        &crate::improvement::ApplicationAuthority::Explicit,
    )
    .await
    .unwrap_err();
    assert!(blocked.contains("release is required"));
    assert!(script.lock().unwrap().1.is_empty());
    writer_state
        .portfolio
        .trace_finish_request(blocked_request, "error", &json!({"error":blocked}))
        .unwrap();

    let release_request = "explicit-create-source-release";
    source_state
        .portfolio
        .trace_begin(
            release_request,
            &json!({"messages":[{"role":"user","content":"Release the create requirements"}]}),
            "improvement",
        )
        .unwrap();
    crate::improvement::declassify_suggestion(&source_state, suggestion_id, release_request)
        .unwrap();
    source_state
        .portfolio
        .trace_finish_request(release_request, "completed", &json!({"released":true}))
        .unwrap();

    let apply_request = "explicit-create-apply-after-release";
    writer_state
        .portfolio
        .trace_begin(
            apply_request,
            &json!({"messages":[{"role":"user","content":"Apply the released create requirements"}]}),
            "improvement",
        )
        .unwrap();
    let applied = crate::improvement::apply_suggestion_async_with_authority(
        &writer_state,
        share_id,
        apply_request,
        &crate::improvement::ApplicationAuthority::Explicit,
    )
    .await
    .unwrap();
    assert_eq!(applied["applied"], true);
    assert_eq!(applied["action"], "create");
    let entity_id = applied["entity_id"].as_str().unwrap();
    assert!(writer_state.portfolio.get(entity_id).unwrap().is_some());
    writer_state
        .portfolio
        .trace_finish_request(apply_request, "completed", &applied)
        .unwrap();

    let conn = base.portfolio.conn.lock().unwrap();
    let (receipt_owner, receipt_workforce, receipt_id, suggestion_status, job_status): (
        String,
        String,
        String,
        String,
        String,
    ) = conn
        .query_row(
            "SELECT r.owner_key_id,r.workforce_id,r.entity_id,s.status,j.status
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
                ))
            },
        )
        .unwrap();
    assert_eq!(receipt_owner, source.key_id);
    assert_eq!(receipt_workforce, source.workforce_id);
    assert_eq!(receipt_id, entity_id);
    assert_eq!(suggestion_status, "applied");
    assert_eq!(job_status, "completed");
    assert_eq!(script.lock().unwrap().1.len(), 1);
}

#[tokio::test]
async fn auto_private_create_uses_the_durable_plan_and_replays_its_source_receipt() {
    let (provider, script) = upstream(vec![message(&definition().to_string())]).await;
    let base = state(&provider.url, false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "auto create source",
            None,
            "auto create workforce",
            &private_improvement_permissions(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let job_id = "auto-private-create-job";
    let suggestion_id = "auto-private-create-suggestion";
    let conversation_id = "auto-private-create-conversation";
    let checkpoint_id = "auto-private-create-checkpoint";
    let requirements = "Create a reusable agent for the private writing gap";
    let proposal = json!({"action":"create","kind":"agent","requirements":requirements});
    let now = crate::observe::now_ms();
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
    {
        let conn = state.portfolio.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
             definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
             auto_apply_private,conversation_id,checkpoint_id,job_type)
             VALUES(?1,'auto-create-origin','auto-create-run','agent','observed-agent','{}',
             'task','result',?2,'suggested',?3,?3,?4,?5,1,?6,?7,'response')",
            rusqlite::params![
                job_id,
                epoch,
                now,
                auth.workforce_id,
                auth.key_id,
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
                auth.workforce_id,
                auth.key_id
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
                auth.workforce_id,
                auth.key_id,
                proposal.to_string(),
                now,
                uuid::Uuid::new_v4().simple().to_string(),
                format!("{job_id}:create:agent:0")
            ],
        )
        .unwrap();
    }
    let authority = private_auto_authority(&auth, job_id);
    let request = "auto-private-create-apply";
    begin_auto_apply_request(&state, request);
    let created = crate::registry::create_with_authority(
        &state,
        request,
        "agent",
        requirements,
        state.portfolio.generation(),
        Some((job_id, 0)),
        &authority,
    )
    .await
    .unwrap();
    let entity_id = created["id"].as_str().unwrap();
    state
        .portfolio
        .trace_finish_request(request, "completed", &created)
        .unwrap();

    let replay_request = "auto-private-create-replay";
    begin_auto_apply_request(&state, replay_request);
    let replay = crate::registry::create_with_authority(
        &state,
        replay_request,
        "agent",
        requirements,
        state.portfolio.generation(),
        Some((job_id, 0)),
        &authority,
    )
    .await
    .unwrap();
    assert_eq!(replay["id"], entity_id);
    state
        .portfolio
        .trace_finish_request(replay_request, "completed", &replay)
        .unwrap();

    let conn = state.portfolio.conn.lock().unwrap();
    let (receipt_owner, receipt_id, suggestion_entity, suggestion_status, job_status, count): (
        String,
        String,
        String,
        String,
        String,
        i64,
    ) = conn
        .query_row(
            "SELECT r.owner_key_id,r.entity_id,s.entity_id,s.status,j.status,
             (SELECT COUNT(*) FROM creation_receipts WHERE job_id=?1 AND order_index=0)
             FROM creation_receipts r
             JOIN improvement_suggestions s ON s.job_id=r.job_id
             JOIN improvement_jobs j ON j.id=r.job_id
             WHERE r.job_id=?1 AND s.id=?2",
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
    assert_eq!(receipt_owner, auth.key_id);
    assert_eq!(receipt_id, entity_id);
    assert_eq!(suggestion_entity, entity_id);
    assert_eq!(suggestion_status, "applied");
    assert_eq!(job_status, "completed");
    assert_eq!(count, 1);
    assert_eq!(script.lock().unwrap().1.len(), 1);
}

#[test]
fn checkpoint_defaults_are_sparse_and_token_checks_need_a_threshold() {
    let config = crate::improvement::ImprovementConfig::default();
    assert!(config.turns.enabled);
    assert_eq!(config.turns.initial_interval, 10);
    assert_eq!(config.turns.minimum_interval, 5);
    assert_eq!(config.turns.maximum_interval, 30);
    assert!(config.elapsed.enabled);
    assert_eq!(config.elapsed.interval_ms, 60 * 60 * 1000);
    assert!(!config.foreground_tokens.enabled);
    assert!(config.foreground_tokens.threshold.is_none());

    let mut token_config = config;
    token_config.foreground_tokens.enabled = true;
    assert!(token_config.validate().is_ok());
    token_config.foreground_tokens.threshold = Some(0);
    assert!(token_config.validate().is_err());
}

#[tokio::test]
async fn elapsed_checkpoint_is_worker_scheduled_and_not_repeated_without_new_prompts() {
    let (provider, script) = upstream(vec![]).await;
    let mut state = state(&provider.url, false);
    let mut config = turn_config(10, 5, 30);
    config.turns.enabled = false;
    config.elapsed.enabled = true;
    set_improvement_config(&mut state, config);
    script.lock().unwrap().0 = vec![
        message(&definition().to_string()),
        message(
            &json!({"trigger":false,"reason":"neutral prompt","feedback":[],"next_turn_interval":10})
                .to_string(),
        ),
    ]
    .into();

    record_turn(
        &state,
        "elapsed-idle-turn",
        "elapsed-idle-conversation",
        "A prompt released before the hourly deadline.",
        None,
        None,
    );
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_conversation_state SET next_elapsed_ms=0
             WHERE conversation_id='elapsed-idle-conversation'",
            [],
        )
        .unwrap();

    assert!(crate::improvement::process_one(&state).await.unwrap());
    let (status, trigger, last_screened, next_elapsed): (String, String, i64, i64) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT c.status,c.triggers,s.last_screened_turn,s.next_elapsed_ms
             FROM improvement_checkpoints c JOIN improvement_conversation_state s
               USING(conversation_id,workforce_id,owner_key_id)
             WHERE c.conversation_id='elapsed-idle-conversation'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(status, "screened_no_trigger");
    assert_eq!(
        serde_json::from_str::<Value>(&trigger).unwrap(),
        json!(["elapsed"])
    );
    assert_eq!(last_screened, 1);
    assert!(next_elapsed > crate::observe::now_ms());
    assert!(!crate::improvement::process_one(&state).await.unwrap());
    assert_eq!(script.lock().unwrap().1.len(), 2);
    assert!(
        script.lock().unwrap().1[1]["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("user_prompts")
    );
}

#[tokio::test]
async fn reset_fences_queued_inflight_and_late_released_checkpoint_evidence() {
    let (provider, script) = upstream(vec![]).await;
    let mut state = state(&provider.url, false);
    set_improvement_config(&mut state, turn_config(10, 5, 30));
    let origin_run = record_turn(
        &state,
        "pre-reset-checkpoint-turn",
        "reset-fenced-conversation",
        "This private pre-reset prompt must not create future consolidation work.",
        None,
        None,
    );
    let late_request = "pre-reset-late-capture";
    let late_payload = json!({"messages":[{"role":"user","content":"This late capture is still pre-reset evidence."}],
        "metadata":{"ccm_conversation_id":"reset-fenced-conversation"}});
    state
        .portfolio
        .trace_begin(late_request, &late_payload, "external")
        .unwrap();
    let old_epoch: i64 = state
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
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms,epoch)
             VALUES('queued-before-reset','reset-fenced-conversation','','',
             'pre-reset-checkpoint-turn',?1,1,'[\"turns\"]','queued',?2,?2,?3)",
            rusqlite::params![origin_run, crate::observe::now_ms(), old_epoch],
        )
        .unwrap();

    state.portfolio.reset().unwrap();
    assert_eq!(
        state
            .portfolio
            .conn
            .lock()
            .unwrap()
            .query_row(
                "SELECT status FROM improvement_checkpoints WHERE id='queued-before-reset'",
                [],
                |row| row.get::<_, String>(0),
            )
            .unwrap(),
        "cancelled"
    );

    let in_flight = crate::improvement::Checkpoint {
        id: "queued-before-reset".into(),
        conversation_id: "reset-fenced-conversation".into(),
        origin_request: "pre-reset-checkpoint-turn".into(),
        origin_run,
        through_turn: 1,
        triggers: "[\"turns\"]".into(),
        workforce_id: String::new(),
        owner_key_id: String::new(),
        epoch: old_epoch,
    };
    assert!(
        !crate::improvement::commit_checkpoint_screening(
            &state.portfolio,
            &in_flight,
            &json!({"trigger":true,"reason":"stale"}),
            true,
            10,
            60_000,
        )
        .unwrap()
    );

    state
        .portfolio
        .capture_conversation_turn(late_request, &late_payload)
        .unwrap();
    state
        .portfolio
        .trace_finish_request(late_request, "completed", &json!({"ok":true}))
        .unwrap();
    state
        .portfolio
        .release_improvements_with_config(late_request, &state.config.improvement)
        .unwrap();

    assert!(!crate::improvement::process_one(&state).await.unwrap());
    let (current_epoch, current_checkpoints, current_consolidation_jobs): (i64, i64, i64) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT (SELECT value FROM improvement_epoch WHERE id=1),
             (SELECT COUNT(*) FROM improvement_checkpoints
               WHERE conversation_id='reset-fenced-conversation' AND epoch=(SELECT value FROM improvement_epoch WHERE id=1)),
             (SELECT COUNT(*) FROM improvement_jobs WHERE job_type='consolidation'
               AND epoch=(SELECT value FROM improvement_epoch WHERE id=1))",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(current_epoch, old_epoch + 1);
    assert_eq!(current_checkpoints, 0);
    assert_eq!(current_consolidation_jobs, 0);
    assert!(script.lock().unwrap().1.is_empty());
}

#[tokio::test]
async fn checkpoint_model_evidence_is_bounded_and_reports_partial_coverage() {
    let (provider, script) = upstream(vec![]).await;
    let mut state = state(&provider.url, false);
    let mut config = turn_config(10, 5, 30);
    config.turns.enabled = false;
    config.elapsed.enabled = false;
    set_improvement_config(&mut state, config);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&definition().to_string()),
        message(
            &json!({
                "trigger":true,
                "reason":"the last prompt cites an ineffective result",
                "feedback":[{"turn":70,"quote":"This result is incorrect"}],
                "next_turn_interval":10
            })
            .to_string(),
        ),
        keep(),
    ]
    .into();

    let mut last_run = String::new();
    for turn in 1..=70 {
        let prompt = if turn == 70 {
            format!("This result is incorrect; {}", "user-context ".repeat(700))
        } else {
            format!("conversation prompt {turn}")
        };
        last_run = record_turn(
            &state,
            &format!("bounded-turn-{turn}"),
            "bounded-conversation",
            &prompt,
            (turn == 70).then_some(&agent),
            None,
        );
    }
    {
        let conn = state.portfolio.conn.lock().unwrap();
        let root_span: String = conn
            .query_row(
                "SELECT root_span FROM trace_requests WHERE id='bounded-turn-70'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE trace_spans SET output=?2 WHERE id=?1",
            rusqlite::params![
                root_span,
                json!("bounded-execution-marker ".repeat(10000)).to_string()
            ],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms)
             VALUES('bounded-checkpoint','bounded-conversation','','','bounded-turn-70',?1,70,
               '[\"manual\"]','queued',?2,?2)",
            rusqlite::params![last_run, crate::observe::now_ms()],
        )
        .unwrap();
    }

    assert!(crate::improvement::process_one(&state).await.unwrap());
    assert!(crate::improvement::process_one(&state).await.unwrap());
    let calls = script.lock().unwrap().1.clone();
    assert!(
        calls[0]["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("agent-generator")
    );
    let screening = calls[1]["messages"][1]["content"].as_str().unwrap();
    assert!(screening.len() <= crate::improvement::MAX_SCREENING_EVIDENCE_BYTES);
    assert!(!screening.contains("bounded-execution-marker"));
    let evaluator = calls[2]["messages"][1]["content"].as_str().unwrap();
    assert!(evaluator.len() <= crate::meta::MAX_SUGGESTION_INPUT_BYTES);
    let evaluator_input: Value = serde_json::from_str(evaluator).unwrap();
    assert_eq!(evaluator_input["coverage"]["complete"], false);
    assert!(evaluator_input["coverage"]["definition_complete"].is_boolean());
    assert!(evaluator.contains("coverage"));
}

#[test]
fn generated_replacement_is_private_until_source_key_explicitly_declassifies_it() {
    let state = state("http://127.0.0.1:1", false);
    let (writer, _) = state
        .portfolio
        .create_key(
            "suggestion writer",
            None,
            "shared suggestion workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: false,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let (source, _) = state
        .portfolio
        .create_key(
            "suggestion source",
            Some(&writer.workforce_id),
            "shared suggestion workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: false,
                roster_write: false,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let writer_state = state.with_auth(&writer);
    let source_state = state.with_auth(&source);
    let agent = insert(&writer_state);
    let suggestion_id = "source-private-suggestion";
    let share_id = "opaque-review-handle";
    let private_marker = "source-private-generated-replacement-marker";
    source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,workforce_id,
             owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,created_ms,share_id)
             VALUES(?1,'private-job','private-checkpoint','private-conversation',?2,?3,'agent',?4,
             'revise',?5,?6,'pending',?7,?8)",
            rusqlite::params![
                suggestion_id,
                source.workforce_id,
                source.key_id,
                agent.id,
                json!({"action":"revise","reason":"private reason","evidence":"private evidence",
                    "replacement":{"provider":"","name":"revised","purpose":"write",
                    "system_prompt":private_marker,"model":"test-model"}}).to_string(),
                json!(agent).to_string(),
                crate::observe::now_ms(),
                share_id
            ],
        )
        .unwrap();

    let before_release = writer_state.portfolio.suggestions().unwrap();
    assert_eq!(before_release.as_array().unwrap().len(), 1);
    assert_eq!(before_release[0]["id"], share_id);
    assert_eq!(before_release[0]["proposed_definition"], Value::Null);
    assert_eq!(before_release[0]["declassification_required"], true);
    assert!(!before_release.to_string().contains(private_marker));
    assert!(!before_release.to_string().contains("private reason"));

    let declassify_request = "source-declassification";
    source_state
        .portfolio
        .trace_begin(
            declassify_request,
            &json!({"messages":[{"role":"user","content":"Release this replacement for workforce review"}]}),
            "improvement",
        )
        .unwrap();
    crate::improvement::declassify_suggestion(&source_state, suggestion_id, declassify_request)
        .unwrap();
    source_state
        .portfolio
        .trace_finish_request(declassify_request, "completed", &json!({"released":true}))
        .unwrap();

    let released = writer_state.portfolio.suggestions().unwrap();
    assert_eq!(
        released[0]["proposed_definition"]["system_prompt"],
        private_marker
    );
    assert_eq!(released[0]["declassification_required"], false);
    assert!(!released.to_string().contains("private reason"));
    let apply_request = "writer-applies-declassified-revision";
    writer_state
        .portfolio
        .trace_begin(
            apply_request,
            &json!({"messages":[{"role":"user","content":"Apply the released revision"}]}),
            "improvement",
        )
        .unwrap();
    let applied =
        crate::improvement::apply_suggestion(&writer_state, share_id, apply_request).unwrap();
    writer_state
        .portfolio
        .trace_finish_request(apply_request, "completed", &applied)
        .unwrap();
    assert_eq!(applied["applied"], true);
    assert_eq!(
        writer_state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt,
        private_marker
    );
    assert!(
        !writer_state
            .portfolio
            .evaluations()
            .unwrap()
            .to_string()
            .contains("private reason")
    );
}

#[tokio::test]
async fn consolidation_is_a_private_suggestion_until_authorized_application() {
    let (provider, script) = upstream(vec![]).await;
    let base = state(&provider.url, false);
    let (auth, _) = base
        .portfolio
        .create_key(
            "consolidation contributor",
            None,
            "consolidation workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let survivor = insert(&state);
    let duplicate = insert(&state);
    let workflow: crate::workflow::Workflow = serde_json::from_value(json!({
        "id":uuid::Uuid::new_v4().to_string(),"name":"future writer","purpose":"write",
        "entry":"write","max_steps":1,"stages":[{"id":"write","agent_id":duplicate.id,
        "instruction":"write accurately","next":null}]
    }))
    .unwrap();
    state
        .portfolio
        .workflow_insert(&workflow, state.portfolio.generation())
        .unwrap();
    state
        .portfolio
        .save_run(
            "pending-consolidation-run",
            &json!({"workflow":workflow,"pending_agent":duplicate}),
            state.portfolio.generation(),
        )
        .unwrap();
    let run = record_turn(
        &state,
        "consolidation-origin-turn",
        "consolidation-conversation",
        "Please review our reusable writing roster.",
        None,
        None,
    );
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms)
             VALUES('consolidation-checkpoint','consolidation-conversation',?1,?2,
             'consolidation-origin-turn',?3,1,'[\"turns\"]','triggered',?4,?4)",
            rusqlite::params![
                auth.workforce_id,
                auth.key_id,
                run,
                crate::observe::now_ms()
            ],
        )
        .unwrap();
    let consolidation_job = "explicit-consolidation-job";
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
             definition,task,result,epoch,status,created_ms,updated_ms,workforce_id,owner_key_id,
             auto_apply,conversation_id,checkpoint_id,job_type)
             VALUES(?1,'consolidation-origin-turn',?2,'consolidation','registry','{}','review','',
             (SELECT value FROM improvement_epoch WHERE id=1),'queued',?3,?3,?4,?5,0,
             'consolidation-conversation','consolidation-checkpoint','consolidation')",
            rusqlite::params![
                consolidation_job,
                run,
                crate::observe::now_ms(),
                auth.workforce_id,
                auth.key_id
            ],
        )
        .unwrap();
    script.lock().unwrap().0 = vec![message(
        &json!({"duplicates":[{
            "kind":"agent","duplicate_id":duplicate.id,"survivor_id":survivor.id,
            "reason":"same reusable contract","evidence":"their full provisions are equivalent"
        }]})
        .to_string(),
    )]
    .into();

    assert!(state.portfolio.get(&duplicate.id).unwrap().is_some());
    assert!(crate::improvement::process_one(&state).await.unwrap());
    assert!(state.portfolio.get(&duplicate.id).unwrap().is_some());
    let suggestions = state.portfolio.suggestions().unwrap();
    assert_eq!(suggestions.as_array().unwrap().len(), 1);
    let suggestion_id = suggestions[0]["id"].as_str().unwrap();
    assert_eq!(suggestions[0]["action"], "consolidate");
    assert_eq!(suggestions[0]["proposal"]["survivor_id"], survivor.id);
    assert_eq!(
        suggestions[0]["proposal"]["evidence"],
        "their full provisions are equivalent"
    );

    let apply_request = "apply-consolidation-suggestion";
    state
        .portfolio
        .trace_begin(
            apply_request,
            &json!({"messages":[{"role":"user","content":"Apply this consolidation suggestion"}]}),
            "improvement",
        )
        .unwrap();
    let applied =
        crate::improvement::apply_suggestion(&state, suggestion_id, apply_request).unwrap();
    state
        .portfolio
        .trace_finish_request(apply_request, "completed", &applied)
        .unwrap();

    assert_eq!(applied["applied"], true);
    assert_eq!(applied["survivor_id"], survivor.id);
    assert!(state.portfolio.get(&duplicate.id).unwrap().is_none());
    assert_eq!(
        state
            .portfolio
            .workflow_get(&workflow.id)
            .unwrap()
            .unwrap()
            .stages[0]
            .agent_id,
        survivor.id
    );
    let pending = state
        .portfolio
        .claim_run("pending-consolidation-run")
        .unwrap();
    assert_eq!(pending["workflow"]["stages"][0]["agent_id"], duplicate.id);
    assert_eq!(
        pending["agents"][&duplicate.id]["system_prompt"],
        duplicate.system_prompt
    );
    let audits = state.portfolio.evaluations().unwrap();
    assert_eq!(audits.as_array().unwrap().len(), 1);
    assert!(
        !audits
            .to_string()
            .contains("their full provisions are equivalent")
    );
    let job_status: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT status FROM improvement_jobs WHERE id=?1",
            [consolidation_job],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(job_status, "suggested");
}

#[tokio::test]
async fn shared_consolidation_preserves_other_keys_legacy_continuations_privately() {
    let base = state("http://127.0.0.1:1", false);
    let (writer, _) = base
        .portfolio
        .create_key(
            "continuation consolidation writer",
            None,
            "continuation workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: false,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let (source, _) = base
        .portfolio
        .create_key(
            "continuation evidence source",
            Some(&writer.workforce_id),
            "continuation workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: false,
                roster_write: true,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    let writer_state = base.with_auth(&writer);
    let source_state = base.with_auth(&source);
    let survivor = insert(&writer_state);
    let duplicate = insert(&writer_state);
    let workflow: crate::workflow::Workflow = serde_json::from_value(json!({
        "id":uuid::Uuid::new_v4().to_string(),"name":"legacy private workflow",
        "purpose":"resume a paused private workflow","entry":"write","max_steps":1,
        "stages":[{"id":"write","agent_id":duplicate.id,
        "instruction":"preserve the original executor","next":null}]
    }))
    .unwrap();
    writer_state
        .portfolio
        .workflow_insert(&workflow, writer_state.portfolio.generation())
        .unwrap();

    let private_marker = "source-key-private-continuation-marker";
    source_state
        .portfolio
        .save_run(
            "source-legacy-workflow-run",
            &json!({"workflow":workflow,"pending_agent":duplicate,"private_state":private_marker}),
            source_state.portfolio.generation(),
        )
        .unwrap();
    let legacy_request = "source-legacy-agent-tool-request";
    let legacy_tool_run = source_state
        .portfolio
        .trace_begin(
            legacy_request,
            &json!({"messages":[{"role":"user","content":"private tool continuation"}]}),
            "external",
        )
        .unwrap();
    source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO trace_tools(id,run_id,status) VALUES(?1,?2,'waiting')",
            rusqlite::params![
                format!("ccm-agent:{}:legacy-call", duplicate.id.replace('-', "")),
                legacy_tool_run
            ],
        )
        .unwrap();

    let suggestion_id = "cross-key-legacy-consolidation";
    let share_id = "cross-key-legacy-consolidation-share";
    let checkpoint_id = "source-private-checkpoint";
    let conversation_id = "source-private-conversation";
    let origin_request = "source-private-request";
    let origin_run = "source-private-run";
    let epoch: i64 = source_state
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
    let now = crate::observe::now_ms();
    source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms,epoch)
             VALUES(?1,?2,?3,?4,?5,?6,1,'[\"turns\"]','running',?7,?7,?8)",
            rusqlite::params![
                checkpoint_id,
                conversation_id,
                source.workforce_id,
                source.key_id,
                origin_request,
                origin_run,
                now,
                epoch
            ],
        )
        .unwrap();
    let checkpoint = crate::improvement::Checkpoint {
        id: checkpoint_id.into(),
        conversation_id: conversation_id.into(),
        origin_request: origin_request.into(),
        origin_run: origin_run.into(),
        through_turn: 1,
        triggers: "[\"turns\"]".into(),
        workforce_id: source.workforce_id.clone(),
        owner_key_id: source.key_id.clone(),
        epoch,
    };
    assert!(
        crate::improvement::commit_checkpoint_screening(
            &source_state.portfolio,
            &checkpoint,
            &json!({"trigger":true}),
            true,
            10,
            3_600_000,
        )
        .unwrap()
    );
    let (source_job_id, auto_apply_private): (String, bool) = source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT id,auto_apply_private FROM improvement_jobs
             WHERE checkpoint_id=?1 AND job_type='consolidation'",
            [checkpoint_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert!(!auto_apply_private);
    source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,workforce_id,
             owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,created_ms,share_id)
             VALUES(?1,?2,?3,?4,?5,?6,'agent',?7,'consolidate',?8,?9,'pending',?10,?11)",
            rusqlite::params![
                suggestion_id,
                source_job_id,
                checkpoint_id,
                conversation_id,
                source.workforce_id,
                source.key_id,
                duplicate.id,
                json!({"action":"consolidate","survivor_id":survivor.id,
                    "survivor_definition":survivor,"reason":private_marker,"evidence":private_marker})
                    .to_string(),
                json!(duplicate).to_string(),
                crate::observe::now_ms(),
                share_id
            ],
        )
        .unwrap();

    assert!(
        crate::improvement::process_one(&source_state)
            .await
            .unwrap()
    );
    let (recovered_job_status, recovered_suggestion_status): (String, String) = source_state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT j.status,s.status FROM improvement_jobs j
             JOIN improvement_suggestions s ON s.job_id=j.id
             WHERE j.id=?1 AND s.id=?2",
            rusqlite::params![source_job_id, suggestion_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(recovered_job_status, "suggested");
    assert_eq!(recovered_suggestion_status, "pending");

    let shared_projection = writer_state.portfolio.suggestions().unwrap();
    assert_eq!(shared_projection[0]["id"], share_id);
    assert!(!shared_projection.to_string().contains(private_marker));
    let apply_request = "writer-applies-private-consolidation";
    writer_state
        .portfolio
        .trace_begin(
            apply_request,
            &json!({"messages":[{"role":"user","content":"Apply this consolidation"}]}),
            "improvement",
        )
        .unwrap();
    let applied =
        crate::improvement::apply_suggestion(&writer_state, share_id, apply_request).unwrap();
    assert_eq!(applied["applied"], true);
    assert!(!applied.to_string().contains(private_marker));
    assert!(
        writer_state
            .portfolio
            .claim_run("source-legacy-workflow-run")
            .is_err()
    );
    assert!(
        writer_state
            .portfolio
            .continuation_agent(&legacy_tool_run)
            .unwrap()
            .is_none()
    );

    let resumed = source_state
        .portfolio
        .claim_run("source-legacy-workflow-run")
        .unwrap();
    assert_eq!(resumed["private_state"], private_marker);
    assert_eq!(
        resumed["agents"][&duplicate.id]["system_prompt"],
        duplicate.system_prompt
    );
    let continuation = source_state
        .portfolio
        .continuation_agent(&legacy_tool_run)
        .unwrap()
        .unwrap();
    assert_eq!(continuation.id, duplicate.id);
    assert_eq!(continuation.system_prompt, duplicate.system_prompt);
}

#[test]
fn policy_owned_work_remains_immediate_and_outside_discretionary_checkpoints() {
    let state = state("http://127.0.0.1:1", false);
    let run = state
        .portfolio
        .trace_begin(
            "mandatory-policy-request",
            &json!({"messages":[{"role":"user","content":"required work"}]}),
            "external",
        )
        .unwrap();
    state
        .portfolio
        .queue_improvement(
            "mandatory-policy-request",
            "workforce",
            "workforce",
            &json!({"phase":"post","orders":[{"kind":"workflow","requirements":"verify required output"}]}),
            "required task",
            "completed answer",
            state.portfolio.generation(),
        )
        .unwrap();
    state
        .portfolio
        .release_improvements("mandatory-policy-request")
        .unwrap();
    assert_eq!(
        state.portfolio.improvements(&run).unwrap()[0]["status"],
        "queued"
    );
    let checkpoint_count: i64 = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM improvement_checkpoints", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(checkpoint_count, 0);
}

#[test]
fn abandoned_async_result_gate_keeps_work_retryable_until_delivery() {
    let state = state("http://127.0.0.1:1", false);
    let agent = insert(&state);
    let request = "retryable-async-result";
    let conversation = "retryable-result-conversation";
    let payload = json!({"messages":[{"role":"user","content":"complete this report"}],
        "metadata":{"ccm_conversation_id":conversation}});
    let run = state
        .portfolio
        .trace_begin(request, &payload, "external")
        .unwrap();
    state
        .portfolio
        .queue_improvement(
            request,
            "agent",
            &agent.id,
            &json!(agent),
            "complete this report",
            "completed answer",
            state.portfolio.generation(),
        )
        .unwrap();
    state
        .portfolio
        .capture_conversation_turn(request, &payload)
        .unwrap();
    state
        .portfolio
        .trace_finish_request(request, "completed", &json!({"ok":true}))
        .unwrap();
    {
        let mut gate = crate::improvement::ResponseGate::new(
            state.portfolio.clone(),
            request.into(),
            state.config.improvement.clone(),
        );
        gate.retain_on_drop();
    }
    assert_eq!(
        state.portfolio.improvements(&run).unwrap()[0]["status"],
        "held"
    );
    let mut retry = crate::improvement::ResponseGate::new(
        state.portfolio.clone(),
        request.into(),
        state.config.improvement.clone(),
    );
    retry.complete();
    assert_eq!(
        state.portfolio.improvements(&run).unwrap()[0]["status"],
        "pending_checkpoint"
    );
}

#[tokio::test]
async fn turn_and_elapsed_triggers_coalesce_and_adaptive_interval_is_bounded() {
    let (provider, script) = upstream(vec![]).await;
    let mut state = state(&provider.url, false);
    let mut config = turn_config(5, 5, 30);
    config.elapsed.enabled = true;
    set_improvement_config(&mut state, config);
    script.lock().unwrap().0 = vec![
        message(&definition().to_string()),
        message(
            &json!({"trigger":false,"reason":"neutral prompts","feedback":[],"next_turn_interval":99}).to_string(),
        ),
    ]
    .into();

    let conversation = "cadence-coalescing";
    for turn in 1..=5 {
        record_turn(
            &state,
            &format!("turn-{turn}"),
            conversation,
            &format!("prompt {turn}"),
            None,
            None,
        );
    }
    let (checkpoint, through, triggers): (i64, i64, String) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),MAX(through_turn),MAX(triggers) FROM improvement_checkpoints
             WHERE conversation_id=?1",
            [conversation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(checkpoint, 1);
    assert_eq!(through, 5);
    assert_eq!(
        serde_json::from_str::<Value>(&triggers).unwrap(),
        json!(["turns"])
    );

    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_conversation_state SET next_elapsed_ms=0
             WHERE conversation_id=?1 AND workforce_id='' AND owner_key_id=''",
            [conversation],
        )
        .unwrap();
    for turn in 6..=10 {
        record_turn(
            &state,
            &format!("turn-{turn}"),
            conversation,
            &format!("prompt {turn}"),
            None,
            None,
        );
    }
    let (count, through, triggers, id): (i64, i64, String, String) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*),MAX(through_turn),MAX(triggers),MAX(id) FROM improvement_checkpoints
             WHERE conversation_id=?1",
            [conversation],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(through, 10);
    assert_eq!(
        serde_json::from_str::<Value>(&triggers).unwrap(),
        json!(["turns", "elapsed"])
    );

    assert!(crate::improvement::process_one(&state).await.unwrap());
    let (status, next_turn_at, screener): (String, i64, String) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT c.status,s.next_turn_at,c.screener FROM improvement_checkpoints c
             JOIN improvement_conversation_state s USING(conversation_id,workforce_id,owner_key_id)
             WHERE c.id=?1",
            [&id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "screened_no_trigger");
    assert_eq!(next_turn_at, 40);
    assert_eq!(
        serde_json::from_str::<Value>(&screener).unwrap()["next_turn_interval"],
        30
    );
    assert!(
        script.lock().unwrap().1[1]["messages"][1]["content"]
            .as_str()
            .unwrap()
            .contains("user_prompts")
    );
}

#[tokio::test]
async fn elapsed_and_token_checkpoints_are_independent_and_missing_usage_is_unknown() {
    let (provider, _) = upstream(vec![]).await;
    let mut state = state(&provider.url, false);
    let mut token_config = turn_config(10, 5, 30);
    token_config.turns.enabled = false;
    token_config.foreground_tokens.enabled = true;
    token_config.foreground_tokens.threshold = Some(50);
    set_improvement_config(&mut state, token_config.clone());

    let conversation = "known-token-usage";
    record_turn(
        &state,
        "token-49",
        conversation,
        "prompt one",
        None,
        Some(49),
    );
    let checkpoints: i64 = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM improvement_checkpoints WHERE conversation_id=?1",
            [conversation],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(checkpoints, 0);
    record_turn(&state, "token-1", conversation, "prompt two", None, Some(1));
    let trigger: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT triggers FROM improvement_checkpoints WHERE conversation_id=?1",
            [conversation],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&trigger).unwrap(),
        json!(["foreground_tokens"])
    );

    let missing_conversation = "missing-token-usage";
    record_turn(
        &state,
        "missing-usage",
        missing_conversation,
        "prompt with unreported usage",
        None,
        None,
    );
    record_turn(
        &state,
        "later-known-usage",
        missing_conversation,
        "later prompt with reported usage",
        None,
        Some(100),
    );
    let (missing_checkpoints, token_total): (i64, Option<i64>) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT (SELECT COUNT(*) FROM improvement_checkpoints WHERE conversation_id=?1),token_total
             FROM improvement_conversation_state WHERE conversation_id=?1 AND workforce_id='' AND owner_key_id=''",
            [missing_conversation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(missing_checkpoints, 0);
    assert_eq!(token_total, None);

    token_config.foreground_tokens.threshold = None;
    set_improvement_config(&mut state, token_config);
    record_turn(
        &state,
        "no-threshold",
        "token-without-threshold",
        "large usage cannot trigger without a threshold",
        None,
        Some(1000),
    );
    let disabled_without_threshold: i64 = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM improvement_checkpoints WHERE conversation_id='token-without-threshold'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(disabled_without_threshold, 0);

    let mut elapsed_config = turn_config(10, 5, 30);
    elapsed_config.turns.enabled = false;
    elapsed_config.elapsed.enabled = true;
    set_improvement_config(&mut state, elapsed_config);
    let elapsed_conversation = "elapsed-only";
    record_turn(
        &state,
        "elapsed-first",
        elapsed_conversation,
        "first prompt",
        None,
        None,
    );
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_conversation_state SET next_elapsed_ms=0
             WHERE conversation_id=?1 AND workforce_id='' AND owner_key_id=''",
            [elapsed_conversation],
        )
        .unwrap();
    record_turn(
        &state,
        "elapsed-second",
        elapsed_conversation,
        "new prompt after elapsed deadline",
        None,
        None,
    );
    let elapsed_trigger: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT triggers FROM improvement_checkpoints WHERE conversation_id=?1",
            [elapsed_conversation],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&elapsed_trigger).unwrap(),
        json!(["elapsed"])
    );
}

#[tokio::test]
async fn automatic_evaluation_waits_for_final_body_completion_in_both_modes() {
    for stream in [false, true] {
        let (provider, script) = upstream(vec![]).await;
        let state = state(&provider.url, false);
        let agent = insert(&state);
        script.lock().unwrap().0 = vec![
            message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
            message("report"),
            message("FINAL ANSWER"),
            keep(),
        ]
        .into();
        let payload = json!({"stream":stream,"messages":[{"role":"user","content":"task"}]});
        let response = handlers::chat_completions(
            State(state.clone()),
            None,
            axum::http::HeaderMap::new(),
            axum::body::Bytes::from(payload.to_string()),
        )
        .await
        .unwrap();
        let run = response.headers()["x-ccm-run-id"]
            .to_str()
            .unwrap()
            .to_string();
        let mut body = response.into_body().into_data_stream();
        let mut text = String::new();
        loop {
            let chunk = body.next().await.unwrap().unwrap();
            text.push_str(std::str::from_utf8(&chunk).unwrap());
            assert!(!crate::improvement::process_one(&state).await.unwrap());
            if (!stream && text.contains("FINAL ANSWER")) || (stream && text.contains("[DONE]")) {
                break;
            }
        }
        assert_eq!(script.lock().unwrap().1.len(), 3);
        assert_eq!(
            state.portfolio.improvements(&run).unwrap()[0]["status"],
            "held"
        );
        assert!(body.next().await.is_none());
        assert_eq!(
            state.portfolio.improvements(&run).unwrap()[0]["status"],
            "pending_checkpoint"
        );
        assert!(!crate::improvement::process_one(&state).await.unwrap());
        assert_eq!(script.lock().unwrap().1.len(), 3);
        let jobs = state.portfolio.improvements(&run).unwrap();
        assert_eq!(jobs[0]["status"], "pending_checkpoint");
        assert!(jobs[0]["evaluation_run"].is_null());
        assert_eq!(run_status(&state, &run), "completed");
    }
}

#[tokio::test]
async fn dropped_body_cancels_held_jobs_without_an_evaluator_call() {
    let (provider, script) = upstream(vec![]).await;
    let state = state(&provider.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        message("report"),
        message("answer"),
    ]
    .into();
    let response = handlers::chat_completions(
        State(state.clone()),
        None,
        axum::http::HeaderMap::new(),
        axum::body::Bytes::from(json!({"messages":[{"role":"user","content":"task"}]}).to_string()),
    )
    .await
    .unwrap();
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    drop(response);
    assert_eq!(
        state.portfolio.improvements(&run).unwrap()[0]["status"],
        "cancelled"
    );
    assert!(!crate::improvement::process_one(&state).await.unwrap());
    assert_eq!(script.lock().unwrap().1.len(), 3);
    let origin: String = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT origin_request FROM improvement_jobs WHERE origin_run=?1",
            [&run],
            |r| r.get(0),
        )
        .unwrap();
    assert!(
        state
            .portfolio
            .queue_improvement(
                &origin,
                "agent",
                &agent.id,
                &json!(agent),
                "task",
                "late report",
                0
            )
            .is_err()
    );
}

#[tokio::test]
async fn composition_failure_never_releases_captured_improvements() {
    for stream in [false, true] {
        let (provider, script) = upstream(vec![]).await;
        let state = state(&provider.url, false);
        let agent = insert(&state);
        script.lock().unwrap().0 = vec![
            message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
            message("report"),
            json!({"__status":503}),
        ]
        .into();
        let response = handlers::chat_completions(
            State(state.clone()),
            None,
            axum::http::HeaderMap::new(),
            axum::body::Bytes::from(
                json!({"stream":stream,"messages":[{"role":"user","content":"task"}]}).to_string(),
            ),
        )
        .await
        .unwrap();
        let run = response.headers()["x-ccm-run-id"]
            .to_str()
            .unwrap()
            .to_string();
        axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(
            state.portfolio.improvements(&run).unwrap()[0]["status"],
            "cancelled"
        );
        assert!(!crate::improvement::process_one(&state).await.unwrap());
        assert_eq!(script.lock().unwrap().1.len(), 3);
    }
}

#[tokio::test]
async fn discretionary_work_waits_for_checkpoint_after_completed_response() {
    let (provider, script) = upstream(vec![]).await;
    let state = state(&provider.url, false);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&json!({"decision":"use","agent_id":agent.id}).to_string()),
        message("report"),
        message("answer"),
    ]
    .into();
    let server = proxy(state.clone()).await;
    let response = request(&server, "task", false).await;
    let run = response.headers()["x-ccm-run-id"]
        .to_str()
        .unwrap()
        .to_string();
    assert_eq!(
        response.json::<Value>().await.unwrap()["choices"][0]["message"]["content"],
        "answer"
    );
    assert!(!crate::improvement::process_one(&state).await.unwrap());
    assert_eq!(
        state.portfolio.improvements(&run).unwrap()[0]["status"],
        "pending_checkpoint"
    );
    assert_eq!(script.lock().unwrap().1.len(), 3);
    assert_eq!(run_status(&state, &run), "completed");
}

#[test]
fn restart_recovers_released_work_but_never_assumes_unsent_response_delivery() {
    let path = std::env::temp_dir().join(format!("ccm-improvement-{}.db", uuid::Uuid::new_v4()));
    let run;
    {
        let portfolio = Portfolio::open(path.to_str().unwrap()).unwrap();
        run = portfolio
            .trace_begin("origin", &json!({"messages":[]}), "external")
            .unwrap();
        for result in ["held", "queued", "running", "completed"] {
            portfolio
                .queue_improvement("origin", "agent", "a", &json!({}), "task", result, 0)
                .unwrap();
            portfolio
                .conn
                .lock()
                .unwrap()
                .execute(
                    "UPDATE improvement_jobs SET status=?1 WHERE result=?1",
                    [result],
                )
                .unwrap();
        }
    }
    {
        let portfolio = Portfolio::open(path.to_str().unwrap()).unwrap();
        let conn = portfolio.conn.lock().unwrap();
        for (result, expected) in [
            ("held", "cancelled"),
            ("queued", "cancelled"),
            ("running", "interrupted"),
            ("completed", "completed"),
        ] {
            let status: String = conn
                .query_row(
                    "SELECT status FROM improvement_jobs WHERE result=?1",
                    [result],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(status, expected);
        }
        drop(conn);
        portfolio.reset().unwrap();
        assert!(
            portfolio
                .improvements(&run)
                .unwrap()
                .as_array()
                .unwrap()
                .iter()
                .all(|j| ["cancelled", "completed", "interrupted"]
                    .contains(&j["status"].as_str().unwrap()))
        );
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn restart_preserves_queued_checkpoints_and_interrupts_an_unfinished_review() {
    let path = std::env::temp_dir().join(format!("ccm-checkpoint-{}.db", uuid::Uuid::new_v4()));
    {
        let portfolio = Portfolio::open(path.to_str().unwrap()).unwrap();
        let conn = portfolio.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO improvement_conversation_state(conversation_id,workforce_id,owner_key_id,
             user_turns,next_turn_at,next_elapsed_ms,token_total,last_screened_turn,updated_ms)
             VALUES('recovery-conversation','','',12,20,123456,42,10,1)",
            [],
        )
        .unwrap();
        for (id, status) in [
            ("queued-checkpoint", "queued"),
            ("running-checkpoint", "running"),
        ] {
            conn.execute(
                "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
                 origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms)
                 VALUES(?1,'recovery-conversation','','','request','run',12,'[\"turns\"]',?2,1,1)",
                rusqlite::params![id, status],
            )
            .unwrap();
        }
        conn.execute(
            "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,workforce_id,
             owner_key_id,entity_kind,entity_id,action,proposal,before_definition,status,created_ms)
             VALUES('recoverable-suggestion','job','queued-checkpoint','recovery-conversation','','',
             'agent','agent','revise','{}','{}','applying',1)",
            [],
        )
        .unwrap();
    }
    {
        let portfolio = Portfolio::open(path.to_str().unwrap()).unwrap();
        let conn = portfolio.conn.lock().unwrap();
        let (turns, next_turn, token_total): (i64, i64, i64) = conn
            .query_row(
                "SELECT user_turns,next_turn_at,token_total FROM improvement_conversation_state
                 WHERE conversation_id='recovery-conversation'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!((turns, next_turn, token_total), (12, 20, 42));
        let checkpoint_status: String = conn
            .query_row(
                "SELECT status FROM improvement_checkpoints WHERE id='running-checkpoint'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            checkpoint_status, "interrupted",
            "a running screener has no durable verdict and must not be replayed"
        );
        let checkpoint_outcome: String = conn
            .query_row(
                "SELECT outcome FROM improvement_checkpoints WHERE id='running-checkpoint'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let checkpoint_outcome: serde_json::Value =
            serde_json::from_str(&checkpoint_outcome).unwrap();
        assert_eq!(checkpoint_outcome["outcome"], "unknown");
        let queued_status: String = conn
            .query_row(
                "SELECT status FROM improvement_checkpoints WHERE id='queued-checkpoint'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(queued_status, "queued");
        let suggestion_status: String = conn
            .query_row(
                "SELECT status FROM improvement_suggestions WHERE id='recoverable-suggestion'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(suggestion_status, "pending");
    }
    std::fs::remove_file(path).unwrap();
}

#[test]
fn startup_recovery_reconciles_receipts_and_interrupts_unproven_running_work() {
    let path =
        std::env::temp_dir().join(format!("ccm-startup-recovery-{}.db", uuid::Uuid::new_v4()));
    {
        let portfolio = Portfolio::open(path.to_str().unwrap()).unwrap();
        let conn = portfolio.conn.lock().unwrap();
        for (id, status, checkpoint, auto_apply_private) in [
            ("no-receipt", "running", Some("checkpoint"), 0),
            ("queued-never-started", "queued", Some("checkpoint"), 0),
            ("applied-receipt", "running", Some("checkpoint"), 0),
            ("pending-non-auto", "running", Some("checkpoint"), 0),
            ("pending-auto", "running", Some("checkpoint"), 1),
            ("earlier-epoch", "running", None, 0),
        ] {
            conn.execute(
                "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
                 definition,task,result,epoch,status,created_ms,updated_ms,checkpoint_id,job_type,
                 auto_apply_private)
                 VALUES(?1,'request','run',?4,'agent','{}','task','result',?5,?2,1,1,?3,'response',?6)",
                rusqlite::params![
                    id,
                    status,
                    checkpoint,
                    if id == "earlier-epoch" { "workforce" } else { "agent" },
                    if id == "earlier-epoch" { -1 } else { 0 },
                    auto_apply_private
                ],
            )
            .unwrap();
        }
        for (id, status) in [
            ("running-checkpoint", "running"),
            ("queued-checkpoint", "queued"),
        ] {
            conn.execute(
                "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
                 origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms)
                 VALUES(?1,'conversation','','','request','run',10,'[\"turns\"]',?2,1,1)",
                rusqlite::params![id, status],
            )
            .unwrap();
        }
        for (id, job, status) in [
            ("applied-suggestion", "applied-receipt", "applied"),
            ("pending-suggestion", "pending-non-auto", "pending"),
            ("auto-pending-suggestion", "pending-auto", "pending"),
        ] {
            conn.execute(
                "INSERT INTO improvement_suggestions(id,job_id,checkpoint_id,conversation_id,
                 workforce_id,owner_key_id,entity_kind,entity_id,action,proposal,before_definition,
                 status,created_ms)
                 VALUES(?1,?2,'checkpoint','conversation','','','agent','agent','revise','{}','{}',?3,1)",
                rusqlite::params![id, job, status],
            )
            .unwrap();
        }
    }
    {
        let portfolio = Portfolio::open(path.to_str().unwrap()).unwrap();
        let conn = portfolio.conn.lock().unwrap();
        let status = |id: &str| -> (String, Option<String>) {
            conn.query_row(
                "SELECT status,outcome FROM improvement_jobs WHERE id=?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap()
        };
        let (no_receipt, outcome) = status("no-receipt");
        assert_eq!(no_receipt, "interrupted");
        let outcome: serde_json::Value = serde_json::from_str(outcome.as_deref().unwrap()).unwrap();
        assert_eq!(outcome["outcome"], "unknown");
        assert_eq!(status("queued-never-started").0, "queued");
        assert_eq!(status("applied-receipt").0, "completed");
        assert_eq!(status("pending-non-auto").0, "suggested");
        assert_eq!(status("pending-auto").0, "queued");
        assert_eq!(status("earlier-epoch").0, "cancelled");
        let checkpoint_status = |id: &str| -> String {
            conn.query_row(
                "SELECT status FROM improvement_checkpoints WHERE id=?1",
                [id],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(checkpoint_status("running-checkpoint"), "interrupted");
        assert_eq!(checkpoint_status("queued-checkpoint"), "queued");
    }
    std::fs::remove_file(path).unwrap();
}

#[tokio::test]
async fn checkpoint_suggestion_stays_private_and_unapplied_until_explicit_application() {
    let (provider, script) = upstream(vec![]).await;
    let mut state = state(&provider.url, false);
    set_improvement_config(&mut state, turn_config(5, 5, 30));
    let (auth, _) = state
        .portfolio
        .create_key(
            "improvement writer",
            None,
            "private improvement workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    // Preserve this scenario's explicit-apply contract on a shared workforce;
    // eligible single-key private workforces now auto-apply these reviews.
    state
        .portfolio
        .create_key(
            "second improvement key",
            Some(&auth.workforce_id),
            "private improvement workforce",
            &crate::auth::KeyPermissions::default(),
        )
        .unwrap();
    let state = state.with_auth(&auth);
    let agent = insert(&state);
    script.lock().unwrap().0 = vec![
        message(&definition().to_string()),
        message(
            &json!({"trigger":true,"reason":"repeated frustration","feedback":[{"turn":5,"quote":"Please keep the scope clear"}],"next_turn_interval":8})
                .to_string(),
        ),
        message(
            &json!({"action":"revise","reason":"scope","evidence":"repeated prompts show missing scope","replacement":definition()})
                .to_string(),
        ),
    ]
    .into();
    let mut runs = Vec::new();
    for turn in 1..=5 {
        runs.push(record_turn(
            &state,
            &format!("review-turn-{turn}"),
            "review-conversation",
            &format!("Please keep the scope clear, turn {turn}"),
            Some(&agent),
            None,
        ));
    }
    assert_eq!(
        state.portfolio.improvements(&runs[0]).unwrap()[0]["status"],
        "pending_checkpoint"
    );
    assert!(crate::improvement::process_one(&state).await.unwrap());
    assert_eq!(
        state.portfolio.improvements(&runs[0]).unwrap()[0]["status"],
        "queued"
    );
    assert!(crate::improvement::process_one(&state).await.unwrap());
    let jobs = state.portfolio.improvements(&runs[0]).unwrap();
    assert_eq!(jobs[0]["status"], "suggested");
    assert_eq!(jobs[0]["outcome"]["applied"], false);
    assert!(jobs[0]["outcome"]["suggestion_id"].is_string());
    let unchanged = state.portfolio.get(&agent.id).unwrap().unwrap();
    assert_eq!(unchanged.system_prompt, agent.system_prompt);
    let suggestions = state.portfolio.suggestions().unwrap();
    assert_eq!(suggestions.as_array().unwrap().len(), 1);
    let suggestion_id = suggestions[0]["id"].as_str().unwrap();
    assert_eq!(suggestions[0]["status"], "pending");

    let apply_request = "explicit-suggestion-application";
    let apply_payload = json!({"messages":[{"role":"user","content":"Apply the reviewed suggestion"}],
        "metadata":{"ccm_conversation_id":"review-conversation"}});
    state
        .portfolio
        .trace_begin(apply_request, &apply_payload, "improvement")
        .unwrap();
    let applied =
        crate::improvement::apply_suggestion(&state, suggestion_id, apply_request).unwrap();
    state
        .portfolio
        .trace_finish_request(apply_request, "completed", &applied)
        .unwrap();
    assert_eq!(applied["applied"], true);
    assert_eq!(
        state.portfolio.suggestions().unwrap()[0]["status"],
        "applied"
    );
    assert_ne!(
        state
            .portfolio
            .get(&agent.id)
            .unwrap()
            .unwrap()
            .system_prompt,
        agent.system_prompt
    );
    assert_eq!(
        state
            .portfolio
            .evaluations()
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn revision_suggestion_receipt_is_idempotent_across_crash_requeue() {
    let (provider, script) = upstream(vec![]).await;
    let mut base = state(&provider.url, false);
    set_improvement_config(&mut base, turn_config(5, 5, 30));
    let (auth, _) = base
        .portfolio
        .create_key(
            "idempotent suggestion contributor",
            None,
            "idempotent suggestion workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                roster_read: true,
                roster_write: true,
                improvement_contribute: true,
                bootstrap_admin: false,
            },
        )
        .unwrap();
    base.portfolio
        .create_key(
            "second idempotency key",
            Some(&auth.workforce_id),
            "idempotent suggestion workforce",
            &crate::auth::KeyPermissions::default(),
        )
        .unwrap();
    let state = base.with_auth(&auth);
    let agent = insert(&state);
    let screener = crate::portfolio::Agent {
        provider: String::new(),
        id: crate::meta::FEEDBACK_SCREENER_ID.into(),
        name: "prompt-only screener".into(),
        purpose: "screen user prompt feedback".into(),
        system_prompt: "Use supplied user-authored prompts only.".into(),
        model: "test-model".into(),
    };
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO improvement_specialists(id,definition,created_ms) VALUES(?1,?2,?3)",
            rusqlite::params![
                screener.id,
                json!(screener).to_string(),
                crate::observe::now_ms()
            ],
        )
        .unwrap();
    script.lock().unwrap().0 = vec![
        message(
            &json!({
                "trigger":true,
                "reason":"the user explicitly requests more accurate reports",
                "feedback":[{"turn":5,"quote":"I need a more accurate report"}],
                "next_turn_interval":10
            })
            .to_string(),
        ),
        message(
            &json!({
                "action":"revise",
                "reason":"the current instructions permit unsupported claims",
                "evidence":"the user asked for a more accurate report",
                "replacement":definition()
            })
            .to_string(),
        ),
    ]
    .into();
    let mut origin_run = String::new();
    for turn in 1..=5 {
        let prompt = if turn == 5 {
            "I need a more accurate report".to_owned()
        } else {
            format!("Please prepare report {turn}.")
        };
        origin_run = record_turn(
            &state,
            &format!("idempotent-turn-{turn}"),
            "idempotent-conversation",
            &prompt,
            (turn == 5).then_some(&agent),
            None,
        );
    }
    assert!(crate::improvement::process_one(&state).await.unwrap());
    assert!(crate::improvement::process_one(&state).await.unwrap());
    let (job_id, status, suggestion_count): (String, String, i64) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT j.id,j.status,
             (SELECT COUNT(*) FROM improvement_suggestions s WHERE s.job_id=j.id)
             FROM improvement_jobs j WHERE j.origin_run=?1 AND j.entity_kind='agent'
               AND j.entity_id=?2",
            rusqlite::params![origin_run, agent.id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "suggested");
    assert_eq!(suggestion_count, 1);
    assert_eq!(script.lock().unwrap().1.len(), 2);

    // Simulate recovery from the former split commit: the suggestion receipt is
    // durable, but the worker receipt was left running. Startup must reconcile
    // from the receipt without another provider call.
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_jobs SET status='running',outcome=NULL WHERE id=?1",
            [&job_id],
        )
        .unwrap();
    state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE improvement_suggestions SET idempotency_key=NULL WHERE job_id=?1",
            [&job_id],
        )
        .unwrap();
    crate::improvement::initialize(&state.portfolio.conn.lock().unwrap()).unwrap();

    let (status, suggestion_count, outcome): (String, i64, String) = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT j.status,(SELECT COUNT(*) FROM improvement_suggestions s WHERE s.job_id=j.id),
             j.outcome FROM improvement_jobs j WHERE j.id=?1",
            [&job_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    assert_eq!(status, "suggested");
    assert_eq!(suggestion_count, 1);
    let outcome: serde_json::Value = serde_json::from_str(&outcome).unwrap();
    assert_eq!(outcome["reconciled"], "suggestion_receipt");
    assert_eq!(
        script.lock().unwrap().1.len(),
        2,
        "startup receipt reconciliation must not call a provider"
    );
}
