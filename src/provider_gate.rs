//! Process-wide admission and recovery, shared by every resolved model/agent state.
use crate::model_config::Provider;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore},
    time::Instant,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Recovery {
    pub cooldown_ms: u64,
    pub max_cooldown_ms: u64,
}
impl Default for Recovery {
    fn default() -> Self {
        Self {
            cooldown_ms: 30_000,
            max_cooldown_ms: 300_000,
        }
    }
}
impl Recovery {
    pub fn validate(&self) -> Result<(), String> {
        if self.cooldown_ms == 0
            || self.max_cooldown_ms < self.cooldown_ms
            || self.max_cooldown_ms > 86_400_000
        {
            return Err(
                "provider recovery requires 0 < cooldown_ms <= max_cooldown_ms <= 86400000".into(),
            );
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct Registry {
    gates: Mutex<BTreeMap<String, Arc<Gate>>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GateSnapshot {
    pub active_requests: usize,
    pub waiting_requests: usize,
    pub circuit_state: &'static str,
    pub cooldown_remaining_ms: Option<u64>,
}

impl Registry {
    pub fn new(providers: &BTreeMap<String, Provider>) -> Self {
        Self {
            gates: Mutex::new(
                providers
                    .iter()
                    .map(|(id, p)| (id.clone(), Arc::new(Gate::new(p))))
                    .collect(),
            ),
        }
    }
    pub fn get(&self, id: &str, provider: &Provider) -> Arc<Gate> {
        self.gates
            .lock()
            .unwrap()
            .entry(id.into())
            .or_insert_with(|| Arc::new(Gate::new(provider)))
            .clone()
    }

    pub fn snapshot(&self) -> BTreeMap<String, GateSnapshot> {
        self.gates
            .lock()
            .unwrap()
            .iter()
            .map(|(id, gate)| (id.clone(), gate.snapshot()))
            .collect()
    }
}

#[derive(Default)]
struct Circuit {
    until: Option<Instant>,
    failures: u32,
    generation: u64,
    probe: bool,
    active: usize,
}
pub struct Gate {
    slots: Arc<Semaphore>,
    recovery: Recovery,
    circuit: Mutex<Circuit>,
    changed: Notify,
    waiting: std::sync::atomic::AtomicUsize,
}
impl Gate {
    fn new(provider: &Provider) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(
                provider
                    .max_concurrent_requests
                    .unwrap_or(Semaphore::MAX_PERMITS),
            )),
            recovery: provider.recovery.clone(),
            circuit: Mutex::new(Circuit::default()),
            changed: Notify::new(),
            waiting: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn snapshot(&self) -> GateSnapshot {
        let circuit = self.circuit.lock().unwrap();
        let now = Instant::now();
        let circuit_state = match circuit.until {
            None => "closed",
            Some(_) if circuit.probe => "half_open",
            Some(until) if until > now => "open",
            Some(_) => "half_open",
        };
        let cooldown_remaining_ms = circuit.until.map(|until| {
            until
                .saturating_duration_since(now)
                .as_millis()
                .min(u64::MAX as u128) as u64
        });
        GateSnapshot {
            active_requests: circuit.active,
            waiting_requests: self.waiting.load(std::sync::atomic::Ordering::Relaxed),
            circuit_state,
            cooldown_remaining_ms,
        }
    }

    pub async fn acquire(self: &Arc<Self>) -> Lease {
        self.waiting
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _waiting = WaitingRequest { gate: self };
        loop {
            // Register before inspecting state so completion cannot lose a wakeup.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let permit = self
                .slots
                .clone()
                .acquire_owned()
                .await
                .expect("provider semaphore never closes");
            let until = {
                let mut c = self.circuit.lock().unwrap();
                let recovering = c.until.is_some();
                if !recovering
                    || (!c.probe && c.active == 0 && c.until.is_some_and(|t| t <= Instant::now()))
                {
                    c.active += 1;
                    c.probe = recovering;
                    drop(_waiting);
                    return Lease {
                        gate: self.clone(),
                        _permit: permit,
                        generation: c.generation,
                        probe: recovering,
                        finished: false,
                    };
                }
                c.until.filter(|t| *t > Instant::now())
            };
            drop(permit); // A cooldown is not an active inference request.
            if let Some(until) = until {
                tokio::select! { _ = tokio::time::sleep_until(until) => {}, _ = &mut changed => {} }
            } else {
                changed.await;
            }
        }
    }
}

struct WaitingRequest<'a> {
    gate: &'a Gate,
}

impl Drop for WaitingRequest<'_> {
    fn drop(&mut self) {
        self.gate
            .waiting
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

pub struct Lease {
    gate: Arc<Gate>,
    _permit: OwnedSemaphorePermit,
    generation: u64,
    probe: bool,
    finished: bool,
}
impl Lease {
    /// Only provider/transport failures open the circuit, not caller cancellation
    /// or local validation. Retry-After is a lower bound, even above our backoff cap.
    pub fn finish(mut self, provider_failed: bool, retry_after_ms: Option<u64>) -> Option<u64> {
        let mut c = self.gate.circuit.lock().unwrap();
        self.finished = true;
        if provider_failed {
            c.failures = c.failures.saturating_add(1);
            c.generation = c.generation.wrapping_add(1);
            let delay = self
                .gate
                .recovery
                .cooldown_ms
                .saturating_mul(1u64 << (c.failures - 1).min(20))
                .min(self.gate.recovery.max_cooldown_ms)
                .max(retry_after_ms.unwrap_or(0));
            let until = Instant::now() + Duration::from_millis(delay);
            c.until = Some(c.until.map_or(until, |old| old.max(until)));
            Some(delay)
        } else {
            if self.probe && self.generation == c.generation {
                c.until = None;
                c.failures = 0;
            }
            None
        }
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut c = self.gate.circuit.lock().unwrap();
        c.active -= 1;
        if self.probe {
            c.probe = false;
            if !self.finished {
                let until = Instant::now() + Duration::from_millis(self.gate.recovery.cooldown_ms);
                c.until = Some(c.until.map_or(until, |old| old.max(until)));
            }
        }
        drop(c);
        self.gate.changed.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn gate(limit: Option<usize>) -> Arc<Gate> {
        Arc::new(Gate::new(&Provider {
            max_concurrent_requests: limit,
            recovery: Recovery {
                cooldown_ms: 25,
                max_cooldown_ms: 100,
            },
            ..Default::default()
        }))
    }
    #[tokio::test]
    async fn capacity_is_shared_and_cancelled_waiters_do_not_leak_slots() {
        let gate = gate(Some(1));
        let first = gate.acquire().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.acquire())
                .await
                .is_err()
        );
        drop(first);
        let second = tokio::time::timeout(Duration::from_millis(100), gate.acquire())
            .await
            .unwrap();
        assert_eq!(gate.circuit.lock().unwrap().active, 1);
        second.finish(false, None);
        assert_eq!(gate.circuit.lock().unwrap().active, 0);
    }
    #[tokio::test]
    async fn cooldown_allows_one_probe_then_releases_waiters_only_on_success() {
        let gate = gate(None);
        gate.acquire().await.finish(true, Some(50));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), gate.acquire())
                .await
                .is_err()
        );
        let probe = gate.acquire().await;
        assert!(probe.probe);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.acquire())
                .await
                .is_err()
        );
        assert_eq!(probe.finish(true, None), Some(50));
        let next = gate.acquire().await;
        next.finish(false, None);
        let a = gate.acquire().await;
        let b = gate.acquire().await;
        assert!(!a.probe && !b.probe);
        assert_eq!(gate.circuit.lock().unwrap().failures, 0);
    }
    #[tokio::test]
    async fn stale_success_does_not_close_circuit_and_cancelled_probe_recovers() {
        let gate = gate(None);
        let a = gate.acquire().await;
        let b = gate.acquire().await;
        a.finish(true, None);
        b.finish(false, None);
        assert!(gate.circuit.lock().unwrap().until.is_some());
        let probe = gate.acquire().await;
        drop(probe);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), gate.acquire())
                .await
                .is_err()
        );
        gate.acquire().await.finish(false, None);
        assert!(gate.circuit.lock().unwrap().until.is_none());
    }
    #[tokio::test]
    async fn providers_are_independent_and_existing_registry_overrides_old_snapshots() {
        let p = Provider {
            max_concurrent_requests: Some(1),
            ..Default::default()
        };
        let registry = Registry::new(&BTreeMap::from([("a".into(), p.clone()), ("b".into(), p)]));
        let a = registry.get("a", &Provider::default());
        let same = registry.get("a", &Provider::default());
        assert!(Arc::ptr_eq(&a, &same));
        let _a = a.acquire().await;
        let _b = registry.get("b", &Provider::default()).acquire().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(10), same.acquire())
                .await
                .is_err()
        );
    }
    #[test]
    fn recovery_defaults_and_invalid_settings() {
        assert_eq!(Recovery::default().cooldown_ms, 30_000);
        for (cooldown_ms, max_cooldown_ms) in [(0, 1), (10, 1), (1, 86_400_001)] {
            assert!(
                Recovery {
                    cooldown_ms,
                    max_cooldown_ms
                }
                .validate()
                .is_err()
            );
        }
        let p: Provider =
            serde_json::from_value(serde_json::json!({"base_url":"http://localhost/v1"})).unwrap();
        assert!(p.max_concurrent_requests.is_none());
        assert_eq!(p.recovery.max_cooldown_ms, 300_000);
    }
}
