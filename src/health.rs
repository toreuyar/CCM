//! Provider-independent process lifecycle and health probes.
//!
//! The public probes intentionally disclose only a coarse lifecycle status.
//! Authenticated runtime and persistence diagnostics live on their existing API
//! surfaces instead.

use std::sync::{Arc, Mutex};

use axum::{Extension, Json, Router, extract::State, http::StatusCode, routing::get};
use serde_json::{Value, json};
use tokio::sync::Notify;

use crate::state::AppState;

const STARTING: u8 = 0;
const READY: u8 = 1;
const DRAINING: u8 = 2;

/// Monotonic service lifecycle shared by health, management, and admission.
/// A drain remains in effect until process restart; an aborted administrative
/// drain should be completed by starting a fresh service rather than silently
/// reopening admission.
pub struct Lifecycle {
    state: Mutex<LifecycleState>,
    admissions_changed: Notify,
}

struct LifecycleState {
    phase: u8,
    active_admissions: usize,
    shutdown_sealed: bool,
}

/// A lease covering a new-work admission until it is registered with the
/// durable async supervisor or its synchronous execution/side effect finishes.
/// It is intentionally non-Clone so one admission has one release point.
#[must_use = "keep the admission permit until work is registered or complete"]
pub struct AdmissionPermit {
    lifecycle: Arc<Lifecycle>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionError {
    Draining,
}

impl AdmissionError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Draining => "service_draining",
        }
    }

    pub const fn is_retryable(self) -> bool {
        true
    }
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Draining => f.write_str("service is draining; retry after the service is ready"),
        }
    }
}

impl std::error::Error for AdmissionError {}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let became_idle = {
            let mut state = self.lifecycle.state.lock().unwrap();
            debug_assert!(state.active_admissions > 0);
            state.active_admissions = state.active_admissions.saturating_sub(1);
            state.active_admissions == 0
        };
        if became_idle {
            self.lifecycle.admissions_changed.notify_waiters();
        }
    }
}

impl Lifecycle {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(LifecycleState {
                phase: STARTING,
                active_admissions: 0,
                shutdown_sealed: false,
            }),
            admissions_changed: Notify::new(),
        }
    }

    /// Stable, explicitly serialized lifecycle value.
    pub fn phase(&self) -> &'static str {
        phase_name(self.state.lock().unwrap().phase)
    }

    pub fn is_accepting(&self) -> bool {
        self.state.lock().unwrap().phase == READY
    }

    /// Atomically admit new work while the service is ready. The same mutex
    /// serializes this decision with `begin_drain`, so a returned permit always
    /// linearizes before drain and remains counted until dropped.
    pub fn try_admit_new_work(self: &Arc<Self>) -> Result<AdmissionPermit, AdmissionError> {
        let mut state = self.state.lock().unwrap();
        if state.phase != READY || state.shutdown_sealed {
            return Err(AdmissionError::Draining);
        }
        state.active_admissions += 1;
        Ok(AdmissionPermit {
            lifecycle: self.clone(),
        })
    }

    /// Admit a continuation/control operation for work already accepted before
    /// drain. Call only after authenticating and locating that existing work.
    /// Shutdown seals this path once active work reaches a quiescent boundary.
    pub fn try_admit_existing_work(self: &Arc<Self>) -> Result<AdmissionPermit, AdmissionError> {
        let mut state = self.state.lock().unwrap();
        if state.phase == STARTING || state.shutdown_sealed {
            return Err(AdmissionError::Draining);
        }
        state.active_admissions += 1;
        Ok(AdmissionPermit {
            lifecycle: self.clone(),
        })
    }

    pub fn active_admissions(&self) -> usize {
        self.state.lock().unwrap().active_admissions
    }

    /// Wait until all pre-drain admissions either finish or transfer into a
    /// supervisor-owned active/queued operation. Call after `begin_drain` so
    /// ready-state admissions cannot continually extend the wait.
    pub async fn wait_for_admissions(&self) {
        loop {
            let notified = self.admissions_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.active_admissions() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Wait until readiness has been withdrawn. Background process-owned tasks
    /// use this to stop promptly rather than sleeping through their next tick.
    pub async fn wait_until_draining(&self) {
        loop {
            let notified = self.admissions_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.phase() == "draining" {
                return;
            }
            notified.await;
        }
    }

    /// Wait until the forced-shutdown cutoff seals continuations. Cooperative
    /// foreground workers use this to drop execution futures (and therefore their
    /// sandbox process groups) instead of surviving past the cleanup boundary.
    pub async fn wait_until_sealed(&self) {
        loop {
            let notified = self.admissions_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.state.lock().unwrap().shutdown_sealed {
                return;
            }
            notified.await;
        }
    }

    /// Close the authenticated continuation window once drain has reached a
    /// quiescent boundary. Existing permits remain counted and must complete.
    pub fn seal_for_shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        state.phase = DRAINING;
        state.shutdown_sealed = true;
        drop(state);
        self.admissions_changed.notify_waiters();
    }

    /// Close admission. Repeated calls are harmless.
    pub fn begin_drain(&self) {
        self.state.lock().unwrap().phase = DRAINING;
        self.admissions_changed.notify_waiters();
    }

    /// Publish readiness after startup initialization and listener bind.
    /// A signal or management drain that raced startup is never overwritten.
    pub fn mark_ready(&self) {
        let mut state = self.state.lock().unwrap();
        if state.phase == STARTING {
            state.phase = READY;
        }
    }

    /// Safe status projection: no paths, credentials, provider configuration,
    /// or runtime error details. Read once so phase and accepting are coherent.
    pub fn safe_snapshot(&self) -> Value {
        let phase = self.state.lock().unwrap().phase;
        json!({
            "phase": phase_name(phase),
            "accepting": phase == READY,
        })
    }
}

fn phase_name(phase: u8) -> &'static str {
    match phase {
        READY => "ready",
        DRAINING => "draining",
        _ => "starting",
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

pub fn routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness))
        .with_state(state)
}

async fn liveness() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({"status": "alive"})))
}

async fn readiness(
    State(state): State<Arc<AppState>>,
    Extension(lifecycle): Extension<Arc<Lifecycle>>,
) -> (StatusCode, Json<Value>) {
    let ready = lifecycle.is_accepting() && persistence_is_healthy(&state);
    if ready {
        (StatusCode::OK, Json(json!({"status": "ready"})))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"status": "not_ready"})),
        )
    }
}

fn persistence_is_healthy(state: &AppState) -> bool {
    state
        .portfolio
        .storage
        .as_ref()
        .is_none_or(|storage| !persistence_error_is_fatal(&storage.metrics()["error"]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_has_stable_snapshot_and_drain_cannot_be_undone_by_startup() {
        let lifecycle = Lifecycle::new();
        assert_eq!(lifecycle.phase(), "starting");
        assert_eq!(
            lifecycle.safe_snapshot(),
            json!({"phase":"starting","accepting":false})
        );

        lifecycle.begin_drain();
        lifecycle.mark_ready();
        assert_eq!(lifecycle.phase(), "draining");
        assert!(!lifecycle.is_accepting());

        let lifecycle = Lifecycle::new();
        lifecycle.mark_ready();
        assert_eq!(lifecycle.phase(), "ready");
        assert!(lifecycle.is_accepting());
        lifecycle.begin_drain();
        lifecycle.begin_drain();
        assert_eq!(lifecycle.phase(), "draining");
    }

    #[test]
    fn concurrent_admission_and_drain_are_linearized_and_successful_leases_are_counted() {
        for _ in 0..128 {
            let lifecycle = Arc::new(Lifecycle::new());
            lifecycle.mark_ready();
            let gate = Arc::new(std::sync::Barrier::new(3));

            let admit_lifecycle = lifecycle.clone();
            let admit_gate = gate.clone();
            let admission = std::thread::spawn(move || {
                admit_gate.wait();
                admit_lifecycle.try_admit_new_work()
            });

            let drain_lifecycle = lifecycle.clone();
            let drain_gate = gate.clone();
            let drain = std::thread::spawn(move || {
                drain_gate.wait();
                drain_lifecycle.begin_drain();
            });

            gate.wait();
            let permit = admission.join().unwrap();
            drain.join().unwrap();

            assert_eq!(lifecycle.phase(), "draining");
            assert!(!lifecycle.is_accepting());
            assert_eq!(
                lifecycle.safe_snapshot(),
                json!({"phase":"draining","accepting":false})
            );
            match lifecycle.try_admit_new_work() {
                Err(error) => {
                    assert_eq!(error.code(), "service_draining");
                    assert!(error.is_retryable());
                }
                Ok(_) => panic!("new work was admitted after drain linearized"),
            }
            assert_eq!(permit.is_ok(), lifecycle.active_admissions() == 1);
            drop(permit);
            assert_eq!(lifecycle.active_admissions(), 0);
        }
    }

    #[tokio::test]
    async fn drain_waits_for_admission_permits_to_be_released() {
        let lifecycle = Arc::new(Lifecycle::new());
        lifecycle.mark_ready();
        let permit = lifecycle.try_admit_new_work().unwrap();
        lifecycle.begin_drain();

        let waiting = {
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move { lifecycle.wait_for_admissions().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        drop(permit);
        waiting.await.unwrap();
        assert_eq!(lifecycle.active_admissions(), 0);
    }

    #[tokio::test]
    async fn drain_notification_wakes_a_waiter_and_handles_an_already_started_drain() {
        let lifecycle = Arc::new(Lifecycle::new());
        lifecycle.mark_ready();
        let waiting = {
            let lifecycle = lifecycle.clone();
            tokio::spawn(async move { lifecycle.wait_until_draining().await })
        };
        tokio::task::yield_now().await;
        assert!(!waiting.is_finished());
        lifecycle.begin_drain();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .expect("drain notification should wake the waiter")
            .unwrap();

        lifecycle.wait_until_draining().await;
    }

    #[test]
    fn existing_work_continuations_are_allowed_during_drain_until_shutdown_seal() {
        let lifecycle = Arc::new(Lifecycle::new());
        lifecycle.mark_ready();
        lifecycle.begin_drain();
        assert!(matches!(
            lifecycle.try_admit_new_work(),
            Err(AdmissionError::Draining)
        ));

        let continuation = lifecycle.try_admit_existing_work().unwrap();
        assert_eq!(lifecycle.active_admissions(), 1);
        lifecycle.seal_for_shutdown();
        assert!(matches!(
            lifecycle.try_admit_existing_work(),
            Err(AdmissionError::Draining)
        ));
        assert_eq!(lifecycle.active_admissions(), 1);
        drop(continuation);
        assert_eq!(lifecycle.active_admissions(), 0);
        assert_eq!(lifecycle.phase(), "draining");
    }

    #[test]
    fn persistence_failure_is_a_readiness_failure_but_batch_lag_is_not() {
        assert!(!persistence_error_is_fatal(&Value::Null));
        assert!(persistence_error_is_fatal(&json!(
            "disk publication failed"
        )));
        // Pending pages, a normal in-progress flush, and generation lag are
        // deliberately not readiness failures by themselves.
        let metrics = json!({
            "error": null,
            "pending_pages": 12,
            "flushing": true,
            "commit_generation": 20,
            "persisted_generation": 15,
        });
        assert!(!persistence_error_is_fatal(&metrics["error"]));
    }
}

fn persistence_error_is_fatal(error: &Value) -> bool {
    !error.is_null()
}
