mod api;
mod async_ops;
mod auth;
mod brief;
mod config;
mod dashboard_cache;
mod debug;
mod error;
mod foreman;
mod foreman_protocol;
mod handlers;
mod health;
mod improvement;
mod loop_runner;
mod management;
mod memory;
mod meta;
mod model_config;
mod models;
mod observe;
mod operations;
mod policy;
mod portfolio;
mod provider_gate;
mod registry;
mod runtime;
#[cfg(test)]
mod scenarios;
mod state;
mod storage;
mod telemetry;
mod traffic;
mod transparency;
mod upstream;
mod web;
mod workflow;
mod workforce;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use crate::config::Config;
use crate::health::Lifecycle;
use crate::portfolio::Portfolio;
use crate::state::AppState;
use axum::Extension;

fn main() {
    // The native sandbox helper must install Landlock/seccomp before Tokio
    // creates worker threads. All regular server startup remains async below.
    if crate::runtime::tools::sandbox_helper_entrypoint() {
        return;
    }
    run_server();
}

#[tokio::main]
async fn run_server() {
    dotenvy::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ccm=info,tower_http=info".into()),
        )
        .init();

    let config = match Config::from_env() {
        Ok(config) => Arc::new(config),
        Err(e) => {
            eprintln!("ccm: {e}");
            std::process::exit(1);
        }
    };
    dashboard_cache::init(config.memory.bytes() / 8);
    if let Err(error) = traffic::init(config.memory.bytes() / 8) {
        eprintln!("ccm: cannot initialize traffic log: {error}");
        std::process::exit(1);
    }

    let http = match reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(config.upstream.connect_timeout_ms))
        .build()
    {
        Ok(client) => client,
        Err(e) => {
            eprintln!("ccm: failed to build http client: {e}");
            std::process::exit(1);
        }
    };

    let portfolio = match Portfolio::open_budget(&config.portfolio_db, config.memory.bytes()) {
        Ok(portfolio) => Arc::new(portfolio),
        Err(e) => {
            eprintln!(
                "ccm: failed to open portfolio at {}: {e}",
                config.portfolio_db
            );
            std::process::exit(1);
        }
    };
    if let Some(store) = &portfolio.storage {
        store.manage_lifecycle();
    }

    let external_api_enabled = match std::env::var("CCM_BOOTSTRAP_ADMIN_KEY") {
        Ok(key) if !key.is_empty() => {
            if let Err(error) = portfolio.seed_bootstrap_key(&key) {
                eprintln!("ccm: bootstrap admin key setup failed: {error}");
                std::process::exit(1);
            }
            true
        }
        _ => {
            tracing::warn!(
                "CCM_BOOTSTRAP_ADMIN_KEY is unset; /api and /v1 are closed until a bootstrap admin key is configured"
            );
            false
        }
    };

    if let Err(error) = portfolio.migrate_model_preferences(&config.model) {
        eprintln!("ccm: model preference migration failed: {error}");
        std::process::exit(1);
    }
    let state = AppState {
        providers: Arc::new(crate::provider_gate::Registry::new(
            &config.routing.providers,
        )),
        config: config.clone(),
        http,
        portfolio,
        external_api_enabled,
        auth: None,
    };

    let state = Arc::new(state);
    let lifecycle = Arc::new(Lifecycle::new());
    state
        .portfolio
        .async_ops
        .attach_lifecycle(lifecycle.clone());
    let app = api::routes(state.clone())
        .merge(health::routes(state.clone()))
        .layer(Extension(lifecycle.clone()));

    let addr: SocketAddr = config.addr();
    let listener = match tokio::net::TcpListener::bind(addr).await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("ccm: failed to bind {addr}: {e}");
            std::process::exit(1);
        }
    };
    lifecycle.mark_ready();
    let mut improvement_worker = crate::improvement::start(state.clone(), lifecycle.clone());
    let maintenance = state.portfolio.clone();
    let maintenance_lifecycle = lifecycle.clone();
    let mut maintenance_worker = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        loop {
            tokio::select! {
                biased;
                _ = maintenance_lifecycle.wait_until_draining() => break,
                _ = interval.tick() => {}
            }
            let Ok(_permit) = maintenance_lifecycle.try_admit_new_work() else {
                break;
            };
            if let Err(error) = maintenance.purge_retired_agents(crate::observe::now_ms()) {
                tracing::error!(%error, "retired agent cleanup failed");
            }
        }
    });

    tracing::info!(
        %addr,
        model = %config.model,
        upstream = %config.upstream_base_url,
        portfolio_db = %config.portfolio_db,
        pid = std::process::id(),
        personality = ?config.personality_transparency,
        total_timeout_ms = config.upstream.total_timeout_ms,
        idle_timeout_ms = config.upstream.idle_timeout_ms,
        "ccm proxy is alive"
    );
    traffic::record(
        None,
        None,
        None,
        "service.started",
        &serde_json::json!({
            "pid":std::process::id(),"address":addr.to_string(),"model":config.model,
            "upstream":config.upstream_base_url,"personality_transparency":config.personality_transparency,
        "provider_budgets":config.upstream,"memory":config.memory
        }),
    );

    let shutdown_state = state.clone();
    let shutdown_budget = config.shutdown.clone();
    let http_grace = shutdown_budget.http_grace();
    // Axum drains every active response, but SSE responses may never finish.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
    });
    let server_result = tokio::select! {
        result = &mut server => Some(result),
        _ = shutdown_signal() => {
            lifecycle.begin_drain();
            let _ = shutdown_tx.send(());
            match tokio::time::timeout(http_grace, &mut server).await {
                Ok(result) => Some(result),
                Err(_) => {
                    tracing::warn!(
                        grace_period_seconds = shutdown_budget.http_grace_secs,
                        "HTTP connection grace elapsed; continuing to drain accepted work"
                    );
                    None
                }
            }
        }
    };
    if server_result.is_some() {
        lifecycle.begin_drain();
    }
    let server_pending = server_result.is_none();
    let mut server_error = server_result.and_then(|result| match result {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error.to_string()),
        Err(error) => Some(format!("HTTP server task failed: {error}")),
    });
    if let Some(error) = &server_error {
        tracing::error!(%error, "HTTP server stopped with an error; continuing bounded drain");
    }

    // New admissions are closed. Existing continuations remain useful during the
    // drain window. The asynchronous drain budget covers all accepted roots,
    // children, queued work, admission leases, maintenance, and improvement work.
    let drain_deadline = tokio::time::Instant::now() + shutdown_budget.async_drain();
    let mut maintenance_error = None;
    let mut improvement_error = None;
    // Each worker handle is consumed by exactly one join. A handle that was
    // already awaited to completion must never be polled again; the explicit
    // joined flags let the timeout path abort/join only handles that are still
    // pending. The timeout future keeps `&mut` borrows rather than moving the
    // handles, so cancellation returns ownership to this function.
    let mut maintenance_joined = false;
    let mut improvement_joined = false;
    let drain_completed = {
        let maintenance_task = &mut maintenance_worker;
        let improvement_task = &mut improvement_worker;
        tokio::time::timeout_at(drain_deadline, async {
            lifecycle.wait_for_admissions().await;
            maintenance_error = maintenance_task
                .await
                .err()
                .map(|error| format!("maintenance worker failed: {error}"));
            maintenance_joined = true;
            state.portfolio.async_ops.wait_for_quiescence().await;
            // Seal continuations only at a quiescent boundary, then account for a
            // continuation that won the race before sealing.
            lifecycle.seal_for_shutdown();
            lifecycle.wait_for_admissions().await;
            state.portfolio.async_ops.wait_for_quiescence().await;
            improvement_error = improvement_task
                .await
                .err()
                .map(|error| format!("improvement worker failed: {error}"));
            improvement_joined = true;
        })
        .await
        .is_ok()
    };
    if let Some(error) = &maintenance_error {
        tracing::error!(%error, "maintenance worker did not exit cleanly");
    }
    if let Some(error) = &improvement_error {
        tracing::error!(%error, "improvement worker did not exit cleanly");
    }

    let cleanup_deadline = tokio::time::Instant::now() + shutdown_budget.cleanup();
    let worker_failed = maintenance_error.is_some() || improvement_error.is_some();
    if !drain_completed || worker_failed {
        if !drain_completed {
            tracing::error!(
                async_drain_seconds = shutdown_budget.async_drain_secs,
                "bounded asynchronous drain expired; interrupting accepted work with unknown outcomes"
            );
        }
        lifecycle.seal_for_shutdown();
        // A worker panic is a cleanup failure, not an early exit: still attempt
        // the critical durable interruption/finalization below. Abort is
        // non-blocking, so durability is attempted before waiting for the worker
        // joins. Only handles that were not already joined are aborted, and every
        // abort/join wait uses the one cleanup deadline instead of adding budget.
        if !improvement_joined {
            improvement_worker.abort();
        }
        if !maintenance_joined {
            maintenance_worker.abort();
        }
        let interrupt_grace = std::cmp::min(shutdown_budget.cleanup(), Duration::from_secs(15));
        let interrupt_deadline = std::cmp::min(
            tokio::time::Instant::now() + interrupt_grace,
            cleanup_deadline,
        );
        // Improvement interruption acquires the SQLite mutex synchronously. Run
        // it off the runtime and bound the wait so a wedged worker cannot escape
        // the shutdown deadline before the critical durable record is attempted.
        let improvement_state = state.clone();
        let improvement_interrupt = tokio::task::spawn_blocking(move || {
            crate::improvement::interrupt_in_flight_for_shutdown(&improvement_state)
        });
        match tokio::time::timeout_at(interrupt_deadline, improvement_interrupt).await {
            Ok(Ok(Ok(count))) => {
                tracing::warn!(
                    interrupted_jobs = count,
                    "improvement work without a durable receipt was recorded as interrupted/unknown"
                );
            }
            Ok(Ok(Err(error))) => {
                tracing::error!(%error, "failed to record interrupted improvement work");
            }
            Ok(Err(error)) => {
                tracing::error!(%error, "improvement interruption worker failed");
            }
            Err(_) => {
                tracing::error!(
                    "improvement interruption exceeded its bounded grace; the worker remains aborted"
                );
            }
        }
        // The supervisor records interrupted/unknown before its durable flush and
        // fences late completion. Signal cancellation so tool/process-group owns
        // their normal cleanup path as their futures are dropped.
        match tokio::time::timeout_at(
            interrupt_deadline,
            state.portfolio.async_ops.interrupt_in_flight_for_shutdown(),
        )
        .await
        {
            Ok(Ok(count)) => {
                tracing::warn!(
                    interrupted_operations = count,
                    "accepted asynchronous operations were recorded as interrupted/unknown"
                );
            }
            Ok(Err(error)) => {
                tracing::error!(%error, "failed to durably record interrupted asynchronous operations");
            }
            Err(_) => {
                tracing::error!(
                    "interrupt publication exceeded its bounded grace; late completions remain fenced"
                );
            }
        }
        if !improvement_joined {
            let _ = tokio::time::timeout_at(cleanup_deadline, &mut improvement_worker).await;
        }
        if !maintenance_joined {
            let _ = tokio::time::timeout_at(cleanup_deadline, &mut maintenance_worker).await;
        }
    }

    if server_pending {
        // Accepted work has reached its terminal/interrupted boundary. Terminate
        // remaining passive/SSE connections rather than letting them hold shutdown.
        server.abort();
        match server.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => server_error = Some(error.to_string()),
            Err(error) if error.is_cancelled() => {}
            Err(error) => server_error = Some(format!("HTTP server task failed: {error}")),
        }
    }

    let mut finalization_failed = false;
    let async_flush_error = match tokio::time::timeout_at(
        cleanup_deadline,
        state.portfolio.async_ops.flush_durable_state(),
    )
    .await
    {
        Ok(Ok(())) => None,
        Ok(Err(error)) => Some(error),
        Err(_) => {
            finalization_failed = true;
            Some("durable async state flush exceeded the cleanup budget".to_string())
        }
    };
    if let Some(error) = &async_flush_error {
        tracing::error!(%error, "durable async state flush failed");
    }

    // Traffic and the Store fence share the same cleanup budget. A timeout cannot
    // cancel a blocking sqlite join, so it is reported as a non-clean exit rather
    // than allowing runtime teardown to wait forever and then claiming success.
    let traffic_error = match tokio::time::timeout_at(
        cleanup_deadline,
        tokio::task::spawn_blocking(crate::traffic::flush),
    )
    .await
    {
        Ok(Ok(Ok(()))) => None,
        Ok(Ok(Err(error))) => Some(error.to_string()),
        Ok(Err(error)) => Some(format!("traffic flush task failed: {error}")),
        Err(_) => {
            finalization_failed = true;
            Some("traffic journal flush exceeded the cleanup budget".to_string())
        }
    };
    if let Some(error) = &traffic_error {
        tracing::error!(%error, "shutdown journal flush failed");
    }

    let store_error = if let Some(store) = shutdown_state.portfolio.storage.clone() {
        match tokio::time::timeout_at(
            cleanup_deadline,
            tokio::task::spawn_blocking(move || store.shutdown()),
        )
        .await
        {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(error))) => Some(error.to_string()),
            Ok(Err(error)) => Some(format!("database shutdown worker failed: {error}")),
            Err(_) => {
                finalization_failed = true;
                Some("database fence/join exceeded the cleanup budget".to_string())
            }
        }
    } else {
        None
    };
    if let Some(error) = &store_error {
        tracing::error!(%error, "database shutdown failed");
    }

    if finalization_failed
        || async_flush_error.is_some()
        || traffic_error.is_some()
        || store_error.is_some()
        || !drain_completed
    {
        eprintln!(
            "ccm: shutdown did not complete cleanly within its bounded budgets; exiting nonzero"
        );
        std::process::exit(1);
    }
    if let Some(error) = server_error {
        eprintln!("ccm: server error: {error}");
        std::process::exit(1);
    }
    if let Some(error) = maintenance_error {
        eprintln!("ccm: {error}");
        std::process::exit(1);
    }
    if let Some(error) = improvement_error {
        eprintln!("ccm: {error}");
        std::process::exit(1);
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
