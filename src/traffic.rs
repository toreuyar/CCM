//! Append-only diagnostic journal. Initialized only by the running service;
//! fixture tests keep all evidence in their isolated trace database.
use serde_json::{Value, json};
use std::{
    fs::OpenOptions,
    io::{BufWriter, Write},
    sync::{Arc, OnceLock, mpsc},
    time::{Duration, Instant},
};

struct Journal {
    queue: mpsc::Sender<Entry>,
    budget: Arc<crate::memory::Budget>,
}
enum Entry {
    Line(String, crate::memory::Lease),
    Flush(mpsc::Sender<Result<(), String>>),
}
static JOURNAL: OnceLock<Journal> = OnceLock::new();

pub fn init(limit: usize) -> Result<(), String> {
    let path = std::env::var("CCM_TRAFFIC_LOG").unwrap_or_else(|_| "ccm-traffic.jsonl".into());
    let path = std::path::Path::new(&path);
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let mut options = OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let (queue, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("ccm-traffic".into())
        .spawn(move || {
            let mut file = BufWriter::with_capacity(64 * 1024, file);
            let mut last = Instant::now();
            loop {
                match receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(Entry::Line(line, _lease)) => {
                        if let Err(error) = file.write_all(line.as_bytes()) {
                            tracing::error!(%error,"traffic journal write failed");
                        }
                    }
                    Ok(Entry::Flush(done)) => {
                        let _ = done.send(file.flush().map_err(|e| e.to_string()));
                    }
                    Err(mpsc::RecvTimeoutError::Disconnected) => {
                        let _ = file.flush();
                        break;
                    }
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                if last.elapsed() >= Duration::from_millis(100) {
                    if let Err(error) = file.flush() {
                        tracing::error!(%error,"traffic journal flush failed");
                    }
                    last = Instant::now();
                }
            }
        })
        .map_err(|e| e.to_string())?;
    JOURNAL
        .set(Journal {
            queue,
            budget: crate::memory::Budget::new(limit.saturating_sub(64 * 1024)),
        })
        .map_err(|_| "traffic logger already initialized")?;
    tracing::info!(path=%path.display(), "full traffic journal enabled");
    Ok(())
}

pub fn record(
    run: Option<&str>,
    span: Option<&str>,
    request: Option<&str>,
    kind: &str,
    data: &Value,
) {
    let Some(journal) = JOURNAL.get() else { return };
    let value = json!({"at_ms":crate::observe::now_ms(),"pid":std::process::id(),
        "run_id":run,"span_id":span,"request_id":request,"kind":kind,"data":crate::observe::redact(data)});
    let mut line = value.to_string();
    line.push('\n');
    match journal.budget.acquire(line.capacity() + 128) {
        Ok(lease) => {
            if journal.queue.send(Entry::Line(line, lease)).is_err() {
                tracing::error!("traffic journal worker unavailable");
            }
        }
        Err(error) => tracing::error!(%error, "traffic journal capacity exceeded"),
    }
}

pub fn flush() -> Result<(), String> {
    let Some(journal) = JOURNAL.get() else {
        return Ok(());
    };
    let (tx, rx) = mpsc::channel();
    journal
        .queue
        .send(Entry::Flush(tx))
        .map_err(|e| e.to_string())?;
    rx.recv().map_err(|e| e.to_string())?
}

pub fn metrics() -> Value {
    JOURNAL
        .get()
        .map(|j| json!({"queued_bytes":j.budget.used(),"limit_bytes":j.budget.limit()}))
        .unwrap_or(Value::Null)
}

pub fn headers(headers: &reqwest::header::HeaderMap) -> Value {
    let entries: Vec<_> = headers.iter().map(|(name, value)| {
        let secret = matches!(name.as_str(), "authorization" | "proxy-authorization" | "cookie" | "set-cookie" | "x-api-key" | "api-key");
        json!({"name":name.as_str(),"value":if secret { "[redacted]".into() } else { String::from_utf8_lossy(value.as_bytes()).into_owned() }})
    }).collect();
    json!(entries)
}

/// HTTP body bytes, before UTF-8/JSON/SSE decoding. Hex is lossless even for
/// invalid UTF-8 or fragments splitting code points; text is a readability aid.
pub fn bytes(bytes: &[u8]) -> Value {
    let mut hex = String::with_capacity(bytes.len() * 2);
    const DIGITS: &[u8] = b"0123456789abcdef";
    for &byte in bytes {
        hex.push(DIGITS[(byte >> 4) as usize] as char);
        hex.push(DIGITS[(byte & 15) as usize] as char);
    }
    json!({"length":bytes.len(),"encoding":"hex","bytes_hex":hex,"text":std::str::from_utf8(bytes).ok()})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_bytes_preserve_invalid_utf8_and_headers_mask_credentials() {
        assert_eq!(bytes(&[0, 0xff, 0xc3])["bytes_hex"], "00ffc3");
        assert!(bytes(&[0xff])["text"].is_null());
        let mut h = reqwest::header::HeaderMap::new();
        h.insert("authorization", "Bearer secret".parse().unwrap());
        h.insert("set-cookie", "session=secret".parse().unwrap());
        h.insert("x-request-id", "correlation".parse().unwrap());
        let logged = headers(&h).to_string();
        assert!(!logged.contains("secret"));
        assert!(logged.contains("correlation"));
    }
}
