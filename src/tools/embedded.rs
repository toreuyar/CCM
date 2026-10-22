use crate::config::ToolingConfig;
use futures::StreamExt;
use serde_json::{Value, json};
use std::{path::Path, process::Stdio, time::Duration};

pub(super) async fn execute(
    config: &ToolingConfig,
    tool: &str,
    arguments: &Value,
) -> Result<Value, String> {
    match tool {
        "file.read" => read_file(config, arguments),
        "file.write" => write_file(config, arguments),
        "shell.run" => run_shell(config, arguments).await,
        "http.request" => request_http(config, arguments).await,
        _ => Err(format!("unknown embedded tool {tool}")),
    }
}

fn workspace(config: &ToolingConfig) -> Result<std::path::PathBuf, String> {
    std::fs::canonicalize(&config.workspace_root)
        .map_err(|error| format!("cannot open configured tools.workspace_root: {error}"))
}

fn relative_path(value: &Value) -> Result<&str, String> {
    let path = value
        .as_str()
        .filter(|path| !path.is_empty())
        .ok_or("file path must be a non-empty string")?;
    if Path::new(path).is_absolute() {
        return Err("absolute file paths are outside the configured workspace".into());
    }
    Ok(path)
}

fn read_file(config: &ToolingConfig, arguments: &Value) -> Result<Value, String> {
    let root = workspace(config)?;
    let relative = Path::new(relative_path(&arguments["path"])?);
    let mut file = open_workspace_file(&root, relative, libc::O_RDONLY, 0)?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file()
        || !single_link(&metadata)
        || metadata.len() > config.max_result_bytes as u64
    {
        return Err("file is not regular or exceeds tools.max_result_bytes".into());
    }
    use std::io::Read;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(config.max_result_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() > config.max_result_bytes {
        return Err("file exceeds tools.max_result_bytes".into());
    }
    let content = String::from_utf8(bytes).map_err(|_| "file is not valid UTF-8")?;
    Ok(json!({"content":content,"bytes":content.len()}))
}

fn write_file(config: &ToolingConfig, arguments: &Value) -> Result<Value, String> {
    let root = workspace(config)?;
    let relative = Path::new(relative_path(&arguments["path"])?);
    let content = arguments["content"]
        .as_str()
        .ok_or("file content must be a string")?;
    if content.len() > config.max_result_bytes {
        return Err("file content exceeds tools.max_result_bytes".into());
    }
    let mut file = open_workspace_file(&root, relative, libc::O_WRONLY | libc::O_CREAT, 0o600)?;
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    if !metadata.is_file() || !single_link(&metadata) {
        return Err("file.write target must be a regular file".into());
    }
    file.set_len(0).map_err(|error| error.to_string())?;
    use std::io::Write;
    file.write_all(content.as_bytes())
        .map_err(|error| error.to_string())?;
    file.flush().map_err(|error| error.to_string())?;
    Ok(json!({"written":content.len()}))
}

#[cfg(unix)]
fn single_link(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    metadata.nlink() == 1
}

#[cfg(not(unix))]
fn single_link(_metadata: &std::fs::Metadata) -> bool {
    true
}

#[cfg(unix)]
fn open_workspace_file(
    root: &Path,
    relative: &Path,
    flags: libc::c_int,
    mode: libc::mode_t,
) -> Result<std::fs::File, String> {
    use std::os::{
        fd::{AsRawFd, FromRawFd},
        unix::ffi::OsStrExt,
    };
    let mut components = Vec::new();
    for component in relative.components() {
        match component {
            std::path::Component::Normal(part) => components.push(
                std::ffi::CString::new(part.as_bytes())
                    .map_err(|_| "file path contains a NUL byte")?,
            ),
            _ => return Err("file path must stay within the configured workspace".into()),
        }
    }
    let target = components.pop().ok_or("file path must name a file")?;
    let mut directory = std::fs::File::open(root).map_err(|error| error.to_string())?;
    for component in components {
        // SAFETY: component is NUL-terminated, the descriptor remains owned by
        // `directory`, and the returned descriptor is checked before transfer.
        let fd = unsafe {
            libc::openat(
                directory.as_raw_fd(),
                component.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0,
            )
        };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        // SAFETY: openat returned a fresh owned descriptor.
        directory = unsafe { std::fs::File::from_raw_fd(fd) };
    }
    // SAFETY: target is NUL-terminated and the parent descriptor is owned/valid.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            target.as_ptr(),
            flags | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
            mode,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    // SAFETY: openat returned a fresh owned descriptor.
    Ok(unsafe { std::fs::File::from_raw_fd(fd) })
}

#[cfg(not(unix))]
fn open_workspace_file(
    _root: &Path,
    _relative: &Path,
    _flags: libc::c_int,
    _mode: libc::mode_t,
) -> Result<std::fs::File, String> {
    Err("workspace file operations are unsupported on this platform".into())
}

async fn run_shell(config: &ToolingConfig, arguments: &Value) -> Result<Value, String> {
    if !config.shell_enabled && config.shell_sandbox_executable.is_none() {
        return Err("shell.run is unavailable until tools.shell_enabled is true".into());
    }
    let sandbox_mode = super::resolve_sandbox_mode(config, None)?;
    let root = workspace(config)?;
    let script = arguments["command"]
        .as_str()
        .filter(|script| !script.trim().is_empty())
        .ok_or("shell command must not be empty")?;
    if script.len() > 4096 {
        return Err("shell command exceeds 4096 bytes".into());
    }
    let scratch = super::sandbox::SandboxTempDir::new()?;
    let mut target_env = std::collections::BTreeMap::new();
    target_env.insert("PATH".into(), "/usr/bin:/bin".into());
    target_env.insert(
        "TMPDIR".into(),
        scratch.path().to_string_lossy().into_owned(),
    );
    target_env.insert("LC_ALL".into(), "C".into());
    let process = super::sandbox::SandboxCommand::new(
        "/bin/sh",
        &["-c".into(), script.to_owned()],
        &root,
        &[root.clone(), scratch.path().to_path_buf()],
        config.sandbox_limits,
        target_env,
        sandbox_mode,
        Stdio::piped(),
        Stdio::null(),
    )?;
    let deadline = tokio::time::Instant::now() + Duration::from_millis(config.request_timeout_ms);
    let mut child = tokio::time::timeout_at(deadline, process.spawn(false))
        .await
        .map_err(|_| "sandbox command timed out".to_string())??;
    let mut stdout = child.take_stdout().ok_or("sandbox stdout unavailable")?;
    let result = tokio::time::timeout_at(deadline, async {
        let output = super::sandbox::read_bounded(&mut stdout, config.max_result_bytes);
        tokio::pin!(output);
        let wait = child.wait_and_cleanup();
        tokio::pin!(wait);
        tokio::select! {
            output = &mut output => {
                let output = output?;
                let status = wait.await?;
                Ok::<_, String>((status, output))
            }
            status = &mut wait => {
                let status = status?;
                let output = output.await?;
                Ok((status, output))
            }
        }
    })
    .await;
    let (result, output) = match result {
        Ok(Ok((status, output))) => (status, output),
        Ok(Err(error)) => {
            child.kill_and_wait().await;
            return Err(error);
        }
        Err(_) => {
            child.kill_and_wait().await;
            return Err("sandbox command timed out".into());
        }
    };
    Ok(json!({
        "status":result.code(),
        "success":result.success(),
        "stdout":String::from_utf8_lossy(&output)
    }))
}

async fn request_http(config: &ToolingConfig, arguments: &Value) -> Result<Value, String> {
    let method = arguments["method"]
        .as_str()
        .ok_or("HTTP method is required")?;
    let method = match method {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        _ => return Err("HTTP method is not supported".into()),
    };
    let address = arguments["url"].as_str().ok_or("HTTP URL is required")?;
    let url = reqwest::Url::parse(address).map_err(|error| format!("invalid HTTP URL: {error}"))?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err("HTTP URL must be http(s), without credentials or a fragment".into());
    }
    let host = url
        .host_str()
        .ok_or("HTTP URL has no host")?
        .to_ascii_lowercase();
    let port = url
        .port_or_known_default()
        .ok_or("HTTP URL has no effective port")?;
    if !config.http_allowed_hosts.iter().any(|allowed| {
        let address = if allowed.contains("://") {
            allowed.clone()
        } else {
            format!("https://{allowed}")
        };
        reqwest::Url::parse(&address).ok().is_some_and(|parsed| {
            parsed.scheme() == url.scheme()
                && parsed
                    .host_str()
                    .is_some_and(|allowed_host| allowed_host.eq_ignore_ascii_case(&host))
                && parsed.port_or_known_default() == Some(port)
        })
    }) {
        return Err("HTTP host is not in tools.http_allowed_hosts".into());
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_millis(config.request_timeout_ms))
        .build()
        .map_err(|error| error.to_string())?;
    let mut request = client.request(method, url);
    if let Some(body) = arguments.get("body").and_then(Value::as_str) {
        if body.len() > config.max_input_bytes {
            return Err("HTTP request body exceeds tools.max_input_bytes".into());
        }
        request = request.body(body.to_owned());
    }
    if let Some(headers) = arguments.get("headers") {
        for pair in headers.as_array().into_iter().flatten() {
            let name = pair["name"]
                .as_str()
                .ok_or("HTTP header name must be a string")?;
            let value = pair["value"]
                .as_str()
                .ok_or("HTTP header value must be a string")?;
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "host"
                    | "connection"
                    | "transfer-encoding"
                    | "content-length"
                    | "authorization"
                    | "cookie"
                    | "proxy-authorization"
            ) {
                return Err("sensitive or hop-by-hop HTTP headers are not accepted".into());
            }
            request = request.header(name, value);
        }
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    let status = response.status().as_u16();
    if response
        .content_length()
        .is_some_and(|length| length > config.max_result_bytes as u64)
    {
        return Err("HTTP response exceeds tools.max_result_bytes".into());
    }
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| error.to_string())?;
        if body.len().saturating_add(chunk.len()) > config.max_result_bytes {
            return Err("HTTP response exceeds tools.max_result_bytes".into());
        }
        body.extend_from_slice(&chunk);
    }
    let text = String::from_utf8_lossy(&body).into_owned();
    let body = serde_json::from_str(&text).unwrap_or_else(|_| json!(text));
    let result = json!({"status":status,"body":body});
    super::pending_result_cap(config, &result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, routing::post};

    fn temp_root() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("ccm-tools-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn filesystem_tools_are_confined_and_bounded() {
        let root = temp_root();
        let mut config = ToolingConfig {
            workspace_root: root.to_string_lossy().into_owned(),
            max_result_bytes: 8,
            ..Default::default()
        };
        write_file(&config, &json!({"path":"note.txt","content":"safe"})).unwrap();
        assert_eq!(
            read_file(&config, &json!({"path":"note.txt"})).unwrap()["content"],
            "safe"
        );
        assert!(write_file(&config, &json!({"path":"../outside","content":"x"})).is_err());
        assert!(write_file(&config, &json!({"path":"large.txt","content":"012345678"})).is_err());
        assert!(read_file(&config, &json!({"path":"/etc/passwd"})).is_err());
        config.workspace_root = root.to_string_lossy().into_owned();
        std::fs::remove_file(root.join("note.txt")).unwrap();
        std::fs::remove_dir(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn filesystem_write_rejects_symlinks_outside_workspace() {
        use std::os::unix::fs::symlink;
        let root = temp_root();
        let outside = temp_root();
        std::fs::write(outside.join("target"), "untouched").unwrap();
        std::fs::hard_link(outside.join("target"), root.join("hard-link")).unwrap();
        symlink(outside.join("target"), root.join("link")).unwrap();
        symlink(&outside, root.join("escape")).unwrap();
        let config = ToolingConfig {
            workspace_root: root.to_string_lossy().into_owned(),
            ..Default::default()
        };
        assert!(write_file(&config, &json!({"path":"link","content":"bad"})).is_err());
        assert!(write_file(&config, &json!({"path":"hard-link","content":"bad"})).is_err());
        assert!(read_file(&config, &json!({"path":"hard-link"})).is_err());
        assert!(write_file(&config, &json!({"path":"escape/new","content":"bad"})).is_err());
        assert_eq!(
            std::fs::read_to_string(outside.join("target")).unwrap(),
            "untouched"
        );
        std::fs::remove_file(root.join("link")).unwrap();
        std::fs::remove_file(root.join("hard-link")).unwrap();
        std::fs::remove_file(root.join("escape")).unwrap();
        std::fs::remove_file(outside.join("target")).unwrap();
        std::fs::remove_dir(root).unwrap();
        std::fs::remove_dir(outside).unwrap();
    }

    async fn local_http_handler(body: String) -> Json<Value> {
        Json(json!({"received":body}))
    }

    #[tokio::test]
    async fn generic_http_is_local_mocked_and_requires_exact_host_allowlist() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = axum::Router::new().route("/echo", post(local_http_handler));
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let host = address.ip().to_string();
        let mut config = ToolingConfig {
            http_allowed_hosts: vec![format!("http://{host}:{}", address.port())],
            ..Default::default()
        };
        let result = request_http(
            &config,
            &json!({"method":"POST","url":format!("http://{address}/echo"),"body":"local only"}),
        )
        .await
        .unwrap();
        assert_eq!(result["status"], 200);
        assert_eq!(result["body"]["received"], "local only");
        config.http_allowed_hosts = vec!["unlisted.invalid".into()];
        assert!(
            request_http(
                &config,
                &json!({"method":"GET","url":format!("http://{address}/echo")}),
            )
            .await
            .is_err()
        );
        server.abort();
    }

    #[tokio::test]
    async fn shell_requires_explicit_tool_enablement() {
        let config = ToolingConfig::default();
        assert!(
            run_shell(&config, &json!({"command":"true"}))
                .await
                .is_err()
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn disabled_sandbox_policy_runs_opted_in_shell_with_output_and_timeout_bounds() {
        let root = temp_root();
        let mut config = ToolingConfig {
            workspace_root: root.to_string_lossy().into_owned(),
            shell_enabled: true,
            sandbox_enabled: if cfg!(target_os = "linux") {
                Some(false)
            } else {
                None
            },
            ..Default::default()
        };
        let result = run_shell(&config, &json!({"command":"printf unsandboxed"}))
            .await
            .unwrap();
        assert_eq!(result["success"], true);
        assert_eq!(result["stdout"], "unsandboxed");

        config.max_result_bytes = 1024;
        let output_limit = run_shell(
            &config,
            &json!({"command":format!("printf '{}'", "x".repeat(2048))}),
        )
        .await
        .unwrap_err();
        assert!(output_limit.contains("output exceeds"));

        config.request_timeout_ms = 100;
        let timeout = run_shell(&config, &json!({"command":"sleep 5"}))
            .await
            .unwrap_err();
        assert!(timeout.contains("timed out"));
        std::fs::remove_dir_all(root).unwrap();
    }
}
