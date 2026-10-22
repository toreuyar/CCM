use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{Extension, Json, Router, extract::State, routing::post};
use serde_json::json;
use tokio::sync::Notify;

use crate::health::Lifecycle;

#[tokio::test]
async fn public_health_probes_are_minimal_provider_independent_and_follow_drain() {
    let state = super::state("http://127.0.0.1:1/v1", false);
    let lifecycle = Arc::new(Lifecycle::new());
    let app = crate::api::routes(state.clone())
        .merge(crate::health::routes(state))
        .layer(Extension(lifecycle.clone()));
    let server = super::serve(app).await;

    let live = reqwest::get(format!("{}/health/live", server.url))
        .await
        .unwrap();
    assert_eq!(live.status(), reqwest::StatusCode::OK);
    assert_eq!(
        live.json::<serde_json::Value>().await.unwrap(),
        json!({"status":"alive"})
    );

    let not_started = reqwest::get(format!("{}/health/ready", server.url))
        .await
        .unwrap();
    assert_eq!(
        not_started.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        not_started.json::<serde_json::Value>().await.unwrap(),
        json!({"status":"not_ready"})
    );

    lifecycle.mark_ready();
    // The configured provider is intentionally unreachable. Health is process
    // lifecycle/persistence readiness, not a provider connectivity assertion.
    let ready = reqwest::get(format!("{}/health/ready", server.url))
        .await
        .unwrap();
    assert_eq!(ready.status(), reqwest::StatusCode::OK);
    assert_eq!(
        ready.json::<serde_json::Value>().await.unwrap(),
        json!({"status":"ready"})
    );

    lifecycle.begin_drain();
    let draining = reqwest::get(format!("{}/health/ready", server.url))
        .await
        .unwrap();
    assert_eq!(draining.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        draining.json::<serde_json::Value>().await.unwrap(),
        json!({"status":"not_ready"})
    );
    let live_while_draining = reqwest::get(format!("{}/health/live", server.url))
        .await
        .unwrap();
    assert_eq!(live_while_draining.status(), reqwest::StatusCode::OK);

    // The normal API remains authenticated; only the deliberately minimal
    // health paths bypass bearer authentication.
    let protected = reqwest::get(format!("{}/v1/models", server.url))
        .await
        .unwrap();
    assert_eq!(protected.status(), reqwest::StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn readiness_remains_provider_independent_under_concurrent_scripted_outage_load() {
    let scripted = (0..32)
        .map(|_| json!({"__status":503,"error":"controlled fixture outage"}))
        .collect::<Vec<_>>();
    let (provider, requests) = super::upstream(scripted).await;
    let state = super::state(&provider.url, false);
    let agent = state
        .portfolio
        .insert(&super::NewAgent {
            provider: String::new(),
            name: "recovery-check".into(),
            purpose: "controlled lifecycle recovery fixture".into(),
            system_prompt: "Return the requested recovery result without tools.".into(),
            model: "test-model".into(),
        })
        .unwrap();
    requests.lock().unwrap().0.extend([
        super::message(
            &json!({"decision":"use","agent_id":agent.id,"note":"controlled recovery"}).to_string(),
        ),
        super::message("recovered specialist result"),
        super::message("recovered"),
    ]);
    let lifecycle = Arc::new(Lifecycle::new());
    lifecycle.mark_ready();

    let api = Router::new()
        .route(
            "/v1/chat/completions",
            post(crate::handlers::chat_completions),
        )
        .with_state(state.clone());
    let app = api
        .merge(crate::health::routes(state))
        .layer(Extension(lifecycle));
    let server = super::serve(app).await;
    let client = reqwest::Client::new();

    for batch in 0..4 {
        let calls = (0..8).map(|item| {
            let client = client.clone();
            let url = format!("{}/v1/chat/completions", server.url);
            let prompt = format!("controlled outage batch {batch} item {item}");
            tokio::spawn(async move {
                client
                    .post(url)
                    .json(&json!({"model":"test-model","messages":[{"role":"user","content":prompt}]}))
                    .send()
                    .await
                    .unwrap()
                    .status()
            })
        });
        let probes = (0..8).map(|_| {
            let client = client.clone();
            let url = format!("{}/health/ready", server.url);
            tokio::spawn(async move {
                let response = client.get(url).send().await.unwrap();
                let status = response.status();
                let body = response.json::<serde_json::Value>().await.unwrap();
                (status, body)
            })
        });
        for call in calls {
            assert_eq!(
                call.await.unwrap(),
                reqwest::StatusCode::SERVICE_UNAVAILABLE
            );
        }

        for probe in probes {
            let (status, body) = probe.await.unwrap();
            assert_eq!(status, reqwest::StatusCode::OK);
            assert_eq!(body, json!({"status":"ready"}));
        }
    }

    let recovery = client
        .post(format!("{}/v1/chat/completions", server.url))
        .json(&json!({"model":"test-model","messages":[{"role":"user","content":"controlled recovery request"}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(recovery.status(), reqwest::StatusCode::OK);
    assert_eq!(
        recovery.json::<serde_json::Value>().await.unwrap()["choices"][0]["message"]["content"],
        "recovered"
    );
    let ready_after_recovery = client
        .get(format!("{}/health/ready", server.url))
        .send()
        .await
        .unwrap();
    assert_eq!(ready_after_recovery.status(), reqwest::StatusCode::OK);
    assert_eq!(requests.lock().unwrap().1.len(), 35);
}

#[derive(Clone)]
struct HeldProvider {
    responses: Arc<Mutex<VecDeque<serde_json::Value>>>,
    calls: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

async fn held_provider(
    State(provider): State<HeldProvider>,
    Json(_request): Json<serde_json::Value>,
) -> Json<serde_json::Value> {
    let first = provider.calls.fetch_add(1, Ordering::SeqCst) == 0;
    let response = provider
        .responses
        .lock()
        .unwrap()
        .pop_front()
        .expect("unexpected upstream request");
    if first {
        provider.entered.notify_one();
        provider.release.notified().await;
    }
    Json(response)
}

#[tokio::test]
async fn native_chat_holds_admission_in_its_worker_and_both_chat_routes_reject_after_drain() {
    let script = HeldProvider {
        responses: Arc::new(Mutex::new(VecDeque::new())),
        calls: Arc::new(AtomicUsize::new(0)),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let upstream = super::serve(
        Router::new()
            .route("/chat/completions", post(held_provider))
            .with_state(script.clone()),
    )
    .await;
    let state = super::state(&upstream.url, false);
    let agent = state
        .portfolio
        .insert(&super::NewAgent {
            provider: String::new(),
            name: "native-drain-agent".into(),
            purpose: "controlled lifecycle request".into(),
            system_prompt: "Return the requested result without tools.".into(),
            model: "test-model".into(),
        })
        .unwrap();
    script.responses.lock().unwrap().extend([
        super::message(&json!({"decision":"use","agent_id":agent.id,"note":"test"}).to_string()),
        super::message("specialist result"),
        super::message("completed native response"),
    ]);
    let (_, token) = state
        .portfolio
        .create_key(
            "native lifecycle test",
            None,
            "native lifecycle workforce",
            &crate::auth::KeyPermissions {
                roster_use: true,
                ..Default::default()
            },
        )
        .unwrap();
    let lifecycle = Arc::new(Lifecycle::new());
    lifecycle.mark_ready();
    let app = crate::api::routes(state.clone()).layer(Extension(lifecycle.clone()));
    let server = super::serve(app).await;
    let client = reqwest::Client::new();

    let accepted = client
        .post(format!("{}/api/chat", server.url))
        .bearer_auth(&token)
        .json(&json!({"message":"run the controlled native request"}))
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), reqwest::StatusCode::OK);
    assert!(accepted.json::<serde_json::Value>().await.unwrap()["run_id"].is_string());
    tokio::time::timeout(Duration::from_secs(2), script.entered.notified())
        .await
        .expect("native worker should enter the upstream request");
    assert_eq!(lifecycle.active_admissions(), 1);

    lifecycle.begin_drain();
    assert_eq!(lifecycle.active_admissions(), 1);
    for (path, payload) in [
        (
            "/api/chat",
            json!({"message":"must not create a conversation"}),
        ),
        (
            "/v1/chat/completions",
            json!({"messages":[{"role":"user","content":"must not dispatch"}]}),
        ),
    ] {
        let response = client
            .post(format!("{}{}", server.url, path))
            .bearer_auth(&token)
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            reqwest::StatusCode::SERVICE_UNAVAILABLE,
            "{path}"
        );
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["error"]["code"],
            "service_draining"
        );
    }
    let conversations: i64 = state
        .portfolio
        .conn
        .lock()
        .unwrap()
        .query_row("SELECT COUNT(*) FROM conversations", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        conversations, 1,
        "drained requests must have no side effects"
    );
    assert_eq!(lifecycle.active_admissions(), 1);

    script.release.notify_one();
    tokio::time::timeout(Duration::from_secs(4), lifecycle.wait_for_admissions())
        .await
        .expect("native worker must release its admission after persistence");
    assert_eq!(lifecycle.active_admissions(), 0);
    assert_eq!(script.calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn bounded_shutdown_interrupts_active_async_work_and_fences_a_late_completion() {
    let state = super::state("http://127.0.0.1:1", false);
    let request = state
        .portfolio
        .trace_begin(
            "bounded-shutdown-request",
            &json!({"messages":[]}),
            "external",
        )
        .unwrap();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (handle, mut status) = state
        .portfolio
        .async_ops
        .start_root(state.portfolio.clone(), request, 0, async move {
            let _ = release_rx.await;
            Ok(json!({"late":"completion"}))
        })
        .await
        .unwrap();
    assert_eq!(handle.status, "running");

    let interrupted = state
        .portfolio
        .async_ops
        .interrupt_in_flight_for_shutdown()
        .await
        .unwrap();
    assert_eq!(interrupted, 1);
    tokio::time::timeout(Duration::from_secs(2), async {
        while status.borrow().status != "interrupted" {
            status.changed().await.unwrap();
        }
    })
    .await
    .expect("active operation must be recorded as interrupted");
    let snapshot = status.borrow().clone();
    assert_eq!(snapshot.recovery_reason.as_deref(), Some("server_shutdown"));
    assert!(snapshot.error.unwrap().contains("outcome is unknown"));
    assert!(!snapshot.result_available);
    assert!(snapshot.result.is_none());

    // Releasing the provider/execution future after the fence must not overwrite
    // the durable unknown outcome with a completed result.
    let _ = release_tx.send(());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(status.borrow().status, "interrupted");
    assert!(status.borrow().result.is_none());
    assert!(
        state
            .portfolio
            .async_ops
            .interrupt_in_flight_for_shutdown()
            .await
            .unwrap()
            == 0,
        "shutdown interruption is idempotent"
    );
    assert!(state.portfolio.async_ops.drain_status().await.quiescent);
}

#[test]
fn forced_shutdown_leaves_running_improvement_work_unknown_and_startup_does_not_replay_it() {
    let path = std::env::temp_dir().join(format!(
        "ccm-shutdown-improvement-{}.db",
        uuid::Uuid::new_v4()
    ));
    {
        let mut state = super::state("http://127.0.0.1:1", false);
        Arc::get_mut(&mut state).unwrap().portfolio =
            Arc::new(crate::portfolio::Portfolio::open(path.to_str().unwrap()).unwrap());
        let conn = state.portfolio.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO improvement_jobs(id,origin_request,origin_run,entity_kind,entity_id,
             definition,task,result,epoch,status,created_ms,updated_ms,job_type)
             VALUES('shutdown-job','request','run','agent','agent','{}','task','result',
             0,'running',1,1,'response')",
            [],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO improvement_checkpoints(id,conversation_id,workforce_id,owner_key_id,
             origin_request,origin_run,through_turn,triggers,status,created_ms,updated_ms)
             VALUES('shutdown-checkpoint','conversation','','','request','run',10,'[\"turns\"]',
             'running',1,1)",
            [],
        )
        .unwrap();
        drop(conn);
        assert_eq!(
            crate::improvement::interrupt_in_flight_for_shutdown(&state).unwrap(),
            1
        );
    }
    {
        let portfolio = crate::portfolio::Portfolio::open(path.to_str().unwrap()).unwrap();
        let conn = portfolio.conn.lock().unwrap();
        let (job_status, job_outcome): (String, String) = conn
            .query_row(
                "SELECT status,outcome FROM improvement_jobs WHERE id='shutdown-job'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(job_status, "interrupted");
        let outcome: serde_json::Value = serde_json::from_str(&job_outcome).unwrap();
        assert_eq!(outcome["outcome"], "unknown");
        let checkpoint_status: String = conn
            .query_row(
                "SELECT status FROM improvement_checkpoints WHERE id='shutdown-checkpoint'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            checkpoint_status, "interrupted",
            "startup recovery must not requeue work interrupted by bounded shutdown"
        );
    }
    std::fs::remove_file(path).unwrap();
}
