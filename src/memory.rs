//! Byte-bounded ownership permits shared by independent consumers.
use std::sync::{Arc, Condvar, Mutex};
use tokio::sync::Notify;

pub struct Budget {
    limit: usize,
    used: Mutex<usize>,
    changed: Condvar,
    ready: Notify,
}

pub struct Lease {
    budget: Arc<Budget>,
    bytes: usize,
}

impl Budget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            limit,
            used: Mutex::new(0),
            changed: Condvar::new(),
            ready: Notify::new(),
        })
    }
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub fn used(&self) -> usize {
        *self.used.lock().unwrap()
    }
    pub fn try_acquire(self: &Arc<Self>, bytes: usize) -> Option<Lease> {
        let mut used = self.used.lock().unwrap();
        if bytes > self.limit.saturating_sub(*used) {
            return None;
        }
        *used += bytes;
        Some(Lease {
            budget: self.clone(),
            bytes,
        })
    }
    pub fn acquire(self: &Arc<Self>, bytes: usize) -> Result<Lease, String> {
        if bytes > self.limit {
            return Err("record exceeds memory budget".into());
        }
        let mut used = self.used.lock().unwrap();
        while bytes > self.limit - *used {
            used = self.changed.wait(used).unwrap();
        }
        *used += bytes;
        Ok(Lease {
            budget: self.clone(),
            bytes,
        })
    }
    pub async fn acquire_async(self: &Arc<Self>, bytes: usize) -> Result<Lease, String> {
        if bytes > self.limit {
            return Err("record exceeds memory budget".into());
        }
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(lease) = self.try_acquire(bytes) {
                return Ok(lease);
            }
            notified.await;
        }
    }
}

impl Drop for Lease {
    fn drop(&mut self) {
        *self.budget.used.lock().unwrap() -= self.bytes;
        self.budget.changed.notify_all();
        self.budget.ready.notify_waiters();
    }
}

/// Queue bytes, rather than a small token count, determine slow-client pressure.
pub struct BufferedValue {
    pub value: serde_json::Value,
    _lease: Lease,
}

pub async fn relay(
    mut input: tokio::sync::mpsc::Receiver<serde_json::Value>,
    output: tokio::sync::mpsc::UnboundedSender<BufferedValue>,
    budget: Arc<Budget>,
) -> Result<(), String> {
    while let Some(value) = input.recv().await {
        // Conservative allowance for JSON tree allocations and channel metadata.
        let cost = value
            .to_string()
            .len()
            .saturating_mul(4)
            .saturating_add(512);
        let lease = tokio::select! {
            lease = budget.acquire_async(cost) => lease?,
            _ = output.closed() => return Err("client disconnected".into()),
        };
        output
            .send(BufferedValue {
                value,
                _lease: lease,
            })
            .map_err(|_| "client disconnected")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[tokio::test]
    async fn slow_client_does_not_block_producer_before_byte_budget_is_exhausted() {
        let budget = Budget::new(2 * 1024 * 1024);
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let (buffered, mut reader) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(relay(rx, buffered, budget.clone()));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            for n in 0..1000 {
                tx.send(json!({"n":n})).await.unwrap();
            }
            drop(tx);
            worker.await.unwrap().unwrap();
        })
        .await
        .unwrap();
        assert!(budget.used() > 0);
        for n in 0..1000 {
            assert_eq!(reader.recv().await.unwrap().value["n"], n);
        }
        assert!(reader.recv().await.is_none());
        assert_eq!(budget.used(), 0);
    }
    #[tokio::test]
    async fn exhausted_capacity_waits_and_disconnected_client_releases_memory() {
        let budget = Budget::new(1024);
        let held = budget.try_acquire(1024).unwrap();
        assert!(budget.try_acquire(1).is_none());
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let (buffered, reader) = tokio::sync::mpsc::unbounded_channel();
        let worker = tokio::spawn(relay(rx, buffered, budget.clone()));
        tx.send(json!({"n":1})).await.unwrap();
        tokio::task::yield_now().await;
        assert!(!worker.is_finished());
        drop(reader);
        assert!(worker.await.unwrap().is_err());
        drop(held);
        assert_eq!(budget.used(), 0);
    }
}
