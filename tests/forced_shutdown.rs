//! Whole-process regression for the forced-shutdown path.
//!
//! This test starts the real debug binary against a disposable loopback
//! configuration, a disposable SQLite database and a scripted provider that
//! hangs. It then sends SIGTERM and verifies the bounded drain exits nonzero
//! without a panic, publishes the durable interrupted/server_shutdown record,
//! and restarts without replaying provider work. It is intentionally isolated
//! from production configuration, credentials and data.

#![cfg(unix)]

use std::{
    io::{Read, Write},
    net::TcpListener,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use serde_json::{Value, json};

const BUDGETS: &str = r#"{
  "http_grace_secs": 1,
  "async_drain_secs": 2,
  "cleanup_secs": 3,
  "exit_margin_secs": 1,
  "supervisor_stopwait_secs": 20
}"#;

struct TempRoot(PathBuf);

impl TempRoot {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("ccm-forced-shutdown-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct MockProvider {
    port: u16,
    calls: Arc<AtomicUsize>,
    release: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl MockProvider {
    fn start(hang: bool) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let thread_calls = calls.clone();
        let thread_release = release.clone();
        let thread_stop = stop.clone();
        let thread = std::thread::spawn(move || {
            while !thread_stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(value) => value,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(_) => break,
                };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    match stream.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(read) => {
                            request.extend_from_slice(&buffer[..read]);
                            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let content_length = request
                    .split(|byte| *byte == b'\n')
                    .find_map(|line| {
                        let line = String::from_utf8_lossy(line);
                        line.strip_prefix("content-length:")
                            .or_else(|| line.strip_prefix("Content-Length:"))
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map_or(request.len(), |position| position + 4);
                while request.len().saturating_sub(header_end) < content_length {
                    match stream.read(&mut buffer) {
                        Ok(0) | Err(_) => break,
                        Ok(read) => request.extend_from_slice(&buffer[..read]),
                    }
                }
                thread_calls.fetch_add(1, Ordering::SeqCst);
                if hang {
                    while !thread_release.load(Ordering::SeqCst)
                        && !thread_stop.load(Ordering::SeqCst)
                    {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                }
                let body = br#"{"error":"scripted provider released"}"#;
                let response = format!(
                    "HTTP/1.1 503 Service Unavailable\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(response.as_bytes());
                let _ = stream.write_all(body);
            }
        });
        Self {
            port,
            calls,
            release,
            stop,
            thread: Some(thread),
        }
    }

    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl Drop for MockProvider {
    fn drop(&mut self) {
        self.release.store(true, Ordering::SeqCst);
        self.stop.store(true, Ordering::SeqCst);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

struct CCM {
    child: tokio::process::Child,
    log: PathBuf,
}

impl CCM {
    async fn start(
        root: &Path,
        config: &Path,
        db: &Path,
        traffic: &Path,
        port: u16,
        bootstrap: &str,
    ) -> Self {
        let log = root.join(format!("ccm-{port}.log"));
        let file = std::fs::File::create(&log).unwrap();
        std::fs::set_permissions(&log, std::fs::Permissions::from_mode(0o600)).unwrap();
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_ccm"));
        command
            .current_dir(root)
            .env_clear()
            .env("CCM_CONFIG", config)
            .env("CCM_PORTFOLIO_DB", db)
            .env("CCM_BOOTSTRAP_ADMIN_KEY", bootstrap)
            .env("CCM_HOST", "127.0.0.1")
            .env("CCM_PORT", port.to_string())
            .env("CCM_TRAFFIC_LOG", traffic)
            .env("RUST_LOG", "error")
            .stdin(Stdio::null())
            .stdout(Stdio::from(file.try_clone().unwrap()))
            .stderr(Stdio::from(file));
        let child = command.spawn().unwrap();
        Self { child, log }
    }

    fn pid(&self) -> i32 {
        self.child.id().unwrap() as i32
    }

    async fn wait(&mut self, timeout: Duration) -> std::process::ExitStatus {
        tokio::time::timeout(timeout, self.child.wait())
            .await
            .expect("CCM did not exit within the bounded deadline")
            .unwrap()
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }
}

impl Drop for CCM {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn write_config(path: &Path, provider_port: u16) {
    let config = format!(
        r#"{{
  "default_model": "fixture-model",
  "providers": {{"fixture": {{"base_url": "http://127.0.0.1:{provider_port}/v1"}}}},
  "models": {{"fixture-model": {{"provider": "fixture", "model": "fixture-model"}}}},
  "memory": {{"budget_mib": 64}},
  "shutdown": {BUDGETS}
}}"#
    );
    std::fs::write(path, config).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

fn free_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_ready(client: &reqwest::Client, base: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Ok(response) = client.get(format!("{base}/health/ready")).send().await
            && response.status() == reqwest::StatusCode::OK
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("CCM did not become ready");
}

async fn json(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: String,
    token: Option<&str>,
    body: Option<Value>,
) -> (reqwest::StatusCode, Value) {
    let mut request = client.request(method, url);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request.send().await.unwrap();
    let status = response.status();
    let value = response.json().await.unwrap_or(Value::Null);
    (status, value)
}

fn operation_row(db: &Path, id: &str) -> (String, Value) {
    let connection = rusqlite::Connection::open(db).unwrap();
    let (status, snapshot): (String, String) = connection
        .query_row(
            "SELECT status, snapshot_json FROM async_operations WHERE id=?1",
            [id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    (status, serde_json::from_str(&snapshot).unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn forced_sigterm_is_bounded_truthful_and_never_replays_work() {
    let root = TempRoot::new();
    let config1 = root.path().join("phase1.json");
    let config2 = root.path().join("phase2.json");
    let db = root.path().join("fixture.sqlite3");
    let traffic = root.path().join("traffic.jsonl");
    let bootstrap = format!("fixture-{}", uuid::Uuid::new_v4());
    let client = reqwest::Client::new();
    let provider1 = MockProvider::start(true);

    write_config(&config1, provider1.port);
    let port1 = free_port();
    let base1 = format!("http://127.0.0.1:{port1}");
    let mut ccm = CCM::start(root.path(), &config1, &db, &traffic, port1, &bootstrap).await;
    wait_ready(&client, &base1).await;

    let (status, created) = json(
        &client,
        reqwest::Method::POST,
        format!("{base1}/api/admin/keys"),
        Some(&bootstrap),
        Some(json!({
            "label": "forced-shutdown-fixture",
            "workforce_name": "forced shutdown fixture",
            "permissions": {"roster_use": true, "roster_read": true}
        })),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::CREATED, "{created}");
    let token = created["token"].as_str().unwrap().to_owned();

    let (status, accepted) = json(
        &client,
        reqwest::Method::POST,
        format!("{base1}/v1/chat/completions"),
        Some(&token),
        Some(json!({
            "model": "fixture-model",
            "messages": [{"role": "user", "content": "fixture hung work"}],
            "metadata": {"ccm_async": true, "ccm_conversation_id": uuid::Uuid::new_v4().to_string()}
        })),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{accepted}");
    let operation = accepted["operation"]["id"].as_str().unwrap().to_owned();

    let deadline = Instant::now() + Duration::from_secs(10);
    while provider1.calls() == 0 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(provider1.calls() > 0, "provider was never reached");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let (status, value) = json(
            &client,
            reqwest::Method::GET,
            format!("{base1}/api/operations/{operation}"),
            Some(&token),
            None,
        )
        .await;
        if status == reqwest::StatusCode::OK && value["status"] == "running" {
            break;
        }
        assert!(Instant::now() < deadline, "operation did not reach running");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    unsafe {
        libc::kill(ccm.pid(), libc::SIGTERM);
    }
    let started = Instant::now();
    let status = ccm.wait(Duration::from_secs(15)).await;
    assert_eq!(status.code(), Some(1), "forced drain must exit nonzero");
    assert!(
        started.elapsed() < Duration::from_secs(12),
        "forced drain exceeded its application budgets"
    );
    let log = ccm.log();
    assert!(!log.contains("panicked at"), "forced drain panicked: {log}");
    assert!(log.contains("bounded asynchronous drain expired"), "{log}");
    assert!(log.contains("exiting nonzero"), "{log}");

    let (stored, snapshot) = operation_row(&db, &operation);
    assert_eq!(stored, "interrupted");
    assert_eq!(snapshot["status"], "interrupted");
    assert_eq!(snapshot["recovery_reason"], "server_shutdown");
    assert_eq!(snapshot["result_available"], false);
    assert!(snapshot["result"].is_null());
    assert!(
        snapshot["error"]
            .as_str()
            .is_some_and(|error| error.contains("outcome is unknown"))
    );
    drop(provider1);

    // Restart with a provider that would record any replay. Startup recovery
    // must preserve the durable server_shutdown record and make no provider call.
    let provider2 = MockProvider::start(false);
    write_config(&config2, provider2.port);
    let port2 = free_port();
    let base2 = format!("http://127.0.0.1:{port2}");
    let mut ccm2 = CCM::start(root.path(), &config2, &db, &traffic, port2, &bootstrap).await;
    wait_ready(&client, &base2).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(provider2.calls(), 0, "restart replayed provider work");
    let (stored, snapshot) = operation_row(&db, &operation);
    assert_eq!(stored, "interrupted");
    assert_eq!(snapshot["recovery_reason"], "server_shutdown");

    let (status, drained) = json(
        &client,
        reqwest::Method::POST,
        format!("{base2}/api/admin/service/drain"),
        Some(&bootstrap),
        Some(json!({})),
    )
    .await;
    assert_eq!(status, reqwest::StatusCode::ACCEPTED, "{drained}");
    unsafe {
        libc::kill(ccm2.pid(), libc::SIGTERM);
    }
    let status = ccm2.wait(Duration::from_secs(15)).await;
    assert_eq!(
        status.code(),
        Some(0),
        "clean drained shutdown must exit zero"
    );
    drop(provider2);
}
