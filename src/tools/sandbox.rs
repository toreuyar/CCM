//! Shared app-owned confinement for native shell and MCP stdio children.
//!
//! The launcher re-execs CCM before creating a Tokio runtime in the helper.
//! This keeps Landlock and seccomp installation single-threaded and avoids a
//! fallible, allocating `pre_exec` callback.

use crate::config::SandboxLimits;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, ChildStdin, Command},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SandboxMode {
    Required,
    Disabled,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SandboxPlatform {
    Linux,
    MacOs,
    Unsupported,
}

#[derive(Debug, Clone, Copy)]
struct SandboxDecision {
    mode: SandboxMode,
    reason: &'static str,
}

fn sandbox_platform() -> SandboxPlatform {
    if native_sandbox_supported() {
        SandboxPlatform::Linux
    } else if cfg!(target_os = "macos") {
        SandboxPlatform::MacOs
    } else {
        SandboxPlatform::Unsupported
    }
}

fn decide_sandbox_mode(
    global_override: Option<bool>,
    server_override: Option<bool>,
    platform: SandboxPlatform,
) -> SandboxDecision {
    let (requested, source) = if let Some(enabled) = server_override {
        (Some(enabled), "mcp_override")
    } else if let Some(enabled) = global_override {
        (Some(enabled), "global_override")
    } else {
        (None, "platform_default")
    };

    match (platform, requested) {
        (SandboxPlatform::Linux, Some(true)) => SandboxDecision {
            mode: SandboxMode::Required,
            reason: if source == "mcp_override" {
                "mcp_override_enabled"
            } else {
                "explicitly_enabled"
            },
        },
        (SandboxPlatform::Linux, Some(false)) => SandboxDecision {
            mode: SandboxMode::Disabled,
            reason: if source == "mcp_override" {
                "mcp_override_disabled"
            } else {
                "explicitly_disabled"
            },
        },
        (SandboxPlatform::Linux, None) => SandboxDecision {
            mode: SandboxMode::Required,
            reason: "linux_platform_default",
        },
        (SandboxPlatform::MacOs, Some(true)) => SandboxDecision {
            mode: SandboxMode::Unsupported,
            reason: "sandbox_backend_unsupported",
        },
        (SandboxPlatform::MacOs, Some(false)) => SandboxDecision {
            mode: SandboxMode::Disabled,
            reason: if source == "mcp_override" {
                "mcp_override_disabled"
            } else {
                "explicitly_disabled"
            },
        },
        (SandboxPlatform::MacOs, None) => SandboxDecision {
            mode: SandboxMode::Disabled,
            reason: "macos_platform_default",
        },
        (SandboxPlatform::Unsupported, _) => SandboxDecision {
            mode: SandboxMode::Unsupported,
            reason: "native_execution_platform_unsupported",
        },
    }
}

pub(crate) fn resolve_mode(
    global_override: Option<bool>,
    server_override: Option<bool>,
) -> Result<SandboxMode, String> {
    let decision = decide_sandbox_mode(global_override, server_override, sandbox_platform());
    if decision.mode == SandboxMode::Unsupported {
        return Err(format!(
            "native execution is unsupported on this platform ({})",
            decision.reason
        ));
    }
    Ok(decision.mode)
}

pub(crate) fn validate_config(config: &crate::config::ToolingConfig) -> Result<(), String> {
    validate_config_for_platform(config, sandbox_platform())
}

fn validate_config_for_platform(
    config: &crate::config::ToolingConfig,
    platform: SandboxPlatform,
) -> Result<(), String> {
    if config.sandbox_enabled == Some(true)
        && decide_sandbox_mode(config.sandbox_enabled, None, platform).mode
            == SandboxMode::Unsupported
    {
        return Err("native sandboxing is unsupported on this platform".into());
    }

    let shell_configured = config.shell_enabled || config.shell_sandbox_executable.is_some();
    if shell_configured
        && decide_sandbox_mode(config.sandbox_enabled, None, platform).mode
            == SandboxMode::Unsupported
    {
        return Err("native shell execution is unsupported on this platform".into());
    }

    for (server_id, server) in &config.mcp_servers {
        if let crate::config::McpTransportConfig::Stdio { sandbox, .. } = &server.transport
            && decide_sandbox_mode(config.sandbox_enabled, sandbox.enabled, platform).mode
                == SandboxMode::Unsupported
        {
            return Err(format!(
                "native MCP stdio execution for {server_id} is unsupported on this platform"
            ));
        }
    }
    Ok(())
}

fn mode_name(mode: SandboxMode) -> &'static str {
    match mode {
        SandboxMode::Required => "required",
        SandboxMode::Disabled => "disabled",
        SandboxMode::Unsupported => "unsupported",
    }
}

fn decision_value(decision: SandboxDecision) -> serde_json::Value {
    let backend = match decision.mode {
        SandboxMode::Required => Some("landlock_seccomp"),
        SandboxMode::Disabled => Some("native_unconfined"),
        SandboxMode::Unsupported => None,
    };
    serde_json::json!({
        "mode": mode_name(decision.mode),
        "backend": backend,
        "reason": decision.reason,
        "setup_failure_policy": if decision.mode == SandboxMode::Required { Some("fail_closed") } else { None },
        "runtime_support": if decision.mode == SandboxMode::Required {
            Some("Landlock ABI 6+ and launcher setup are checked for each child; failures reject execution")
        } else {
            None
        }
    })
}

pub(crate) fn runtime_status(config: &crate::config::ToolingConfig) -> serde_json::Value {
    let mut stdio = serde_json::Map::new();
    for (server_id, server) in &config.mcp_servers {
        if let crate::config::McpTransportConfig::Stdio { sandbox, .. } = &server.transport {
            stdio.insert(
                server_id.clone(),
                decision_value(decide_sandbox_mode(
                    config.sandbox_enabled,
                    sandbox.enabled,
                    sandbox_platform(),
                )),
            );
        }
    }
    serde_json::json!({
        "shell": decision_value(decide_sandbox_mode(config.sandbox_enabled, None, sandbox_platform())),
        "mcp_stdio": stdio,
    })
}

const HELPER_ARG: &str = "--ccm-internal-sandbox-v1";
const SPEC_MAX_BYTES: usize = 128 * 1024;
const SPEC_VERSION: u32 = 1;
const CLEANUP_GRACE: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize, Deserialize)]
struct SandboxSpec {
    version: u32,
    target: String,
    target_args: Vec<String>,
    cwd: String,
    writable_roots: Vec<String>,
    limits: SandboxLimits,
    target_env: BTreeMap<String, String>,
}

pub(super) struct SandboxTempDir {
    path: PathBuf,
}

impl SandboxTempDir {
    pub(super) fn new() -> Result<Self, String> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::{DirBuilderExt, PermissionsExt};

            let base = std::fs::canonicalize(std::env::temp_dir())
                .map_err(|error| format!("sandbox scratch root is unavailable: {error}"))?;
            if !base.is_dir() {
                return Err("sandbox scratch root is not a directory".into());
            }
            for _ in 0..4 {
                let path = base.join(format!("ccm-sandbox-{}", uuid::Uuid::new_v4()));
                let mut builder = std::fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(&path) {
                    Ok(()) => {
                        if let Err(error) =
                            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700))
                        {
                            let _ = std::fs::remove_dir(&path);
                            return Err(format!(
                                "cannot restrict sandbox scratch directory: {error}"
                            ));
                        }
                        let path = match std::fs::canonicalize(&path) {
                            Ok(path) => path,
                            Err(error) => {
                                let _ = std::fs::remove_dir(&path);
                                return Err(format!(
                                    "cannot resolve sandbox scratch directory: {error}"
                                ));
                            }
                        };
                        return Ok(Self { path });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(error) => {
                        return Err(format!("cannot create sandbox scratch directory: {error}"));
                    }
                }
            }
            Err("cannot allocate a unique sandbox scratch directory".into())
        }
        #[cfg(not(unix))]
        {
            Err("native tool scratch directories are unsupported on this platform".into())
        }
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SandboxTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub(super) struct SandboxCommand {
    process: Command,
    spec_frame: Option<Vec<u8>>,
}

impl SandboxCommand {
    pub(super) fn new(
        executable: &str,
        args: &[String],
        cwd: &Path,
        writable_roots: &[PathBuf],
        limits: SandboxLimits,
        target_env: BTreeMap<String, String>,
        mode: SandboxMode,
        stdout: Stdio,
        stderr: Stdio,
    ) -> Result<Self, String> {
        match mode {
            SandboxMode::Required => {
                let helper = std::env::current_exe()
                    .map_err(|error| format!("cannot locate CCM sandbox helper: {error}"))?;
                Self::new_with_helper(
                    &helper,
                    executable,
                    args,
                    cwd,
                    writable_roots,
                    limits,
                    target_env,
                    stdout,
                    stderr,
                )
            }
            SandboxMode::Disabled => {
                Self::new_unconfined(executable, args, cwd, target_env, stdout, stderr)
            }
            SandboxMode::Unsupported => {
                Err("native execution mode is unsupported on this platform".into())
            }
        }
    }

    fn new_with_helper(
        helper: &Path,
        executable: &str,
        args: &[String],
        cwd: &Path,
        writable_roots: &[PathBuf],
        limits: SandboxLimits,
        mut target_env: BTreeMap<String, String>,
        stdout: Stdio,
        stderr: Stdio,
    ) -> Result<Self, String> {
        if !native_sandbox_supported() {
            return Err(
                "native tool sandboxing requires Linux x86_64/aarch64 and Landlock/seccomp".into(),
            );
        }
        if executable.trim().is_empty() || !Path::new(executable).is_absolute() {
            return Err("sandbox executable must be an absolute path".into());
        }
        if args.iter().any(|arg| arg.as_bytes().contains(&0)) {
            return Err("sandbox argument contains a NUL byte".into());
        }
        for name in target_env.keys() {
            validate_target_env_name(name)?;
        }
        target_env
            .entry("PATH".into())
            .or_insert_with(|| "/usr/bin:/bin".into());

        let cwd = canonical_directory(cwd, "sandbox working directory")?;
        let mut roots = Vec::with_capacity(writable_roots.len());
        for path in writable_roots {
            let path = canonical_directory(path, "sandbox writable root")?;
            if !roots.contains(&path) {
                roots.push(path);
            }
        }
        if roots.is_empty() || !roots.iter().any(|root| root == &cwd) {
            return Err("sandbox working directory must be an explicitly writable root".into());
        }
        if roots.len() > 8 || args.len() > 128 || target_env.len() > 64 {
            return Err("sandbox launch exceeds argument, root, or environment limits".into());
        }
        let spec = SandboxSpec {
            version: SPEC_VERSION,
            target: executable.to_owned(),
            target_args: args.to_vec(),
            cwd: path_string(&cwd)?,
            writable_roots: roots
                .iter()
                .map(|path| path_string(path))
                .collect::<Result<Vec<_>, _>>()?,
            limits,
            target_env,
        };
        let encoded = serde_json::to_vec(&spec).map_err(|error| error.to_string())?;
        if encoded.len() > SPEC_MAX_BYTES {
            return Err("sandbox launch specification exceeds its size limit".into());
        }
        let length = u32::try_from(encoded.len())
            .map_err(|_| "sandbox launch specification is too large")?;
        let mut spec_frame = Vec::with_capacity(4 + encoded.len());
        spec_frame.extend_from_slice(&length.to_be_bytes());
        spec_frame.extend_from_slice(&encoded);

        let mut process = Command::new(helper);
        process
            .arg(HELPER_ARG)
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(stdout)
            .stderr(stderr)
            .kill_on_drop(true);
        #[cfg(unix)]
        process.process_group(0);
        #[cfg(not(unix))]
        return Err("native tool sandboxing is supported only on Linux".into());
        Ok(Self {
            process,
            spec_frame: Some(spec_frame),
        })
    }

    fn new_unconfined(
        executable: &str,
        args: &[String],
        cwd: &Path,
        target_env: BTreeMap<String, String>,
        stdout: Stdio,
        stderr: Stdio,
    ) -> Result<Self, String> {
        if !matches!(
            sandbox_platform(),
            SandboxPlatform::Linux | SandboxPlatform::MacOs
        ) {
            return Err("unconfined native execution is unsupported on this platform".into());
        }
        if executable.trim().is_empty() || !Path::new(executable).is_absolute() {
            return Err("native tool executable must be an absolute path".into());
        }
        if args.iter().any(|arg| arg.as_bytes().contains(&0)) {
            return Err("native tool argument contains a NUL byte".into());
        }
        for name in target_env.keys() {
            validate_target_env_name(name)?;
        }
        let cwd = canonical_directory(cwd, "native tool working directory")?;
        let mut target_env = target_env;
        target_env
            .entry("PATH".into())
            .or_insert_with(|| "/usr/bin:/bin".into());
        let launch_bytes = args.iter().map(String::len).sum::<usize>()
            + target_env
                .iter()
                .map(|(name, value)| name.len().saturating_add(value.len()))
                .sum::<usize>();
        if args.len() > 128
            || target_env.len() > 64
            || launch_bytes > SPEC_MAX_BYTES
            || target_env
                .values()
                .any(|value| value.as_bytes().contains(&0))
        {
            return Err("native launch exceeds argument or environment limits".into());
        }

        let mut process = Command::new(executable);
        process
            .args(args)
            .current_dir(cwd)
            .env_clear()
            .envs(target_env)
            .stdin(Stdio::piped())
            .stdout(stdout)
            .stderr(stderr)
            .kill_on_drop(true);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;

            process.process_group(0);
            let max_fd = unsafe { libc::getdtablesize() }.max(3);
            // Close inherited descriptors in the forked child before exec while
            // keeping stdin/stdout/stderr. `close` is async-signal-safe.
            unsafe {
                process.as_std_mut().pre_exec(move || {
                    close_unconfined_inherited_fds(max_fd);
                    Ok(())
                });
            }
        }
        #[cfg(not(unix))]
        return Err("native unconfined execution requires a supported Unix platform".into());
        Ok(Self {
            process,
            spec_frame: None,
        })
    }

    pub(super) async fn spawn(mut self, preserve_stdin: bool) -> Result<SandboxChild, String> {
        let child = self
            .process
            .spawn()
            .map_err(|error| format!("sandbox helper spawn failed: {error}"))?;
        let pgid = child
            .id()
            .ok_or("sandbox helper did not expose its process id")? as i32;
        let group = ProcessGroupGuard::new(pgid)?;
        let mut sandbox_child = SandboxChild {
            child,
            group,
            stdin: None,
        };
        if let Some(spec_frame) = self.spec_frame {
            let mut stdin = sandbox_child
                .child
                .stdin
                .take()
                .ok_or("sandbox helper stdin pipe unavailable")?;
            stdin.write_all(&spec_frame).await.map_err(|error| {
                format!("sandbox helper rejected its launch specification: {error}")
            })?;
            stdin
                .flush()
                .await
                .map_err(|error| format!("sandbox helper launch handshake failed: {error}"))?;
            if preserve_stdin {
                sandbox_child.stdin = Some(stdin);
            }
        } else if preserve_stdin {
            sandbox_child.stdin = sandbox_child.child.stdin.take();
        } else {
            drop(sandbox_child.child.stdin.take());
        }
        Ok(sandbox_child)
    }
}

pub(super) struct SandboxChild {
    child: Child,
    group: ProcessGroupGuard,
    stdin: Option<ChildStdin>,
}

impl SandboxChild {
    pub(super) fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.stdin.take()
    }

    pub(super) fn take_stdout(&mut self) -> Option<tokio::process::ChildStdout> {
        self.child.stdout.take()
    }

    /// Observe the leader without reaping it, kill any surviving group members,
    /// then reap the leader. Keeping the zombie until after killpg prevents the
    /// process-group id from being recycled before cleanup.
    pub(super) async fn wait_and_cleanup(&mut self) -> Result<std::process::ExitStatus, String> {
        #[cfg(any(
            all(
                target_os = "linux",
                any(target_arch = "x86_64", target_arch = "aarch64")
            ),
            target_os = "macos"
        ))]
        {
            let pid = self
                .child
                .id()
                .ok_or("sandbox child has already been reaped")? as i32;
            tokio::task::spawn_blocking(move || wait_without_reaping(pid))
                .await
                .map_err(|error| format!("sandbox wait worker failed: {error}"))??;
            self.group.kill();
            let status = self
                .child
                .wait()
                .await
                .map_err(|error| format!("sandbox child wait failed: {error}"))?;
            self.group.disarm();
            Ok(status)
        }

        #[cfg(all(
            unix,
            not(any(
                all(
                    target_os = "linux",
                    any(target_arch = "x86_64", target_arch = "aarch64")
                ),
                target_os = "macos"
            ))
        ))]
        {
            // Platforms without waitid(WNOWAIT) get best-effort descendant
            // cleanup after the process-group leader exits.
            let status = self
                .child
                .wait()
                .await
                .map_err(|error| format!("native child wait failed: {error}"))?;
            self.group.kill();
            self.group.disarm();
            Ok(status)
        }

        #[cfg(not(unix))]
        {
            Err("native process cleanup is unsupported on this platform".into())
        }
    }

    pub(super) async fn kill_and_wait(&mut self) {
        self.group.kill();
        match tokio::time::timeout(CLEANUP_GRACE, self.child.wait()).await {
            Ok(Ok(_)) => self.group.disarm(),
            Ok(Err(error)) => tracing::warn!(%error, "sandbox child reap failed after group kill"),
            Err(_) => tracing::warn!("sandbox child reap exceeded cleanup grace"),
        }
    }
}

impl Drop for SandboxChild {
    fn drop(&mut self) {
        self.group.kill();
    }
}

struct ProcessGroupGuard {
    pgid: i32,
    active: bool,
}

impl ProcessGroupGuard {
    fn new(pgid: i32) -> Result<Self, String> {
        #[cfg(not(unix))]
        return Err("native process-group cleanup is unsupported on this platform".into());
        Ok(Self { pgid, active: true })
    }

    fn kill(&self) {
        #[cfg(unix)]
        if self.active {
            // Sandboxed Linux children cannot escape this group; unconfined
            // Unix children receive best-effort group cleanup on cancellation.
            unsafe {
                libc::kill(-self.pgid, libc::SIGKILL);
            }
        }
    }

    fn disarm(&mut self) {
        self.active = false;
    }
}

impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.kill();
    }
}

pub(super) async fn read_bounded(
    reader: &mut (impl AsyncRead + Unpin),
    max_bytes: usize,
) -> Result<Vec<u8>, String> {
    let mut output = Vec::with_capacity(max_bytes.min(16 * 1024));
    let mut buffer = [0u8; 8192];
    loop {
        let read = reader
            .read(&mut buffer)
            .await
            .map_err(|error| format!("sandbox output read failed: {error}"))?;
        if read == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(read) > max_bytes {
            return Err("sandbox output exceeds tools.max_result_bytes".into());
        }
        output.extend_from_slice(&buffer[..read]);
    }
}

pub(super) fn validate_target_env_name(name: &str) -> Result<(), String> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    let dynamic_loader = name.starts_with("LD_")
        || matches!(
            name,
            "GLIBC_TUNABLES" | "GCONV_PATH" | "LOCPATH" | "MALLOC_TRACE"
        );
    if !valid || dynamic_loader || name.starts_with("CCM_SANDBOX_") {
        return Err("sandbox environment variable name is invalid or reserved".into());
    }
    Ok(())
}

pub(crate) fn native_sandbox_supported() -> bool {
    cfg!(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))
}

#[cfg(unix)]
fn close_unconfined_inherited_fds(max_fd: i32) {
    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    if unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) } == 0 {
        return;
    }
    for fd in 3..max_fd {
        unsafe {
            libc::close(fd);
        }
    }
}

pub(super) fn helper_entrypoint() -> bool {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().as_deref() != Some(std::ffi::OsStr::new(HELPER_ARG)) {
        return false;
    }
    match run_helper(args.collect()) {
        Ok(()) => std::process::exit(0),
        Err(error) => {
            eprintln!("ccm sandbox helper: {error}");
            std::process::exit(125);
        }
    }
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf, String> {
    let path =
        std::fs::canonicalize(path).map_err(|error| format!("cannot resolve {label}: {error}"))?;
    if !path.is_dir() {
        return Err(format!("{label} is not a directory"));
    }
    Ok(path)
}

fn path_string(path: &Path) -> Result<String, String> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| "sandbox path is not valid UTF-8".into())
}

#[cfg(any(
    all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    target_os = "macos"
))]
fn wait_without_reaping(pid: i32) -> Result<(), String> {
    let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
    loop {
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            )
        };
        if result == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.kind() != std::io::ErrorKind::Interrupted {
            return Err(format!("cannot observe sandbox process exit: {error}"));
        }
    }
}

#[cfg(not(any(
    all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ),
    target_os = "macos"
)))]
fn wait_without_reaping(_pid: i32) -> Result<(), String> {
    Err("native tool sandboxing is unsupported on this platform".into())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn run_helper(args: Vec<std::ffi::OsString>) -> Result<(), String> {
    if !args.is_empty() {
        return Err("sandbox helper arguments are malformed".into());
    }
    close_inherited_descriptors()?;
    let spec = read_spec_from_stdin()?;
    if spec.version != SPEC_VERSION {
        return Err("sandbox launch specification version is unsupported".into());
    }
    if spec.target_args.len() > 128
        || spec
            .target_args
            .iter()
            .any(|arg| arg.as_bytes().contains(&0))
        || spec.writable_roots.is_empty()
        || spec.writable_roots.len() > 8
        || spec.target_env.len() > 64
        || spec
            .target_env
            .values()
            .any(|value| value.as_bytes().contains(&0))
    {
        return Err("sandbox launch specification exceeds argument/environment limits".into());
    }
    let target_arg = spec.target.as_str();
    if !Path::new(target_arg).is_absolute() {
        return Err("sandbox executable path must be absolute".into());
    }
    if !native_sandbox_supported() {
        return Err("native tool sandboxing is unsupported on this platform".into());
    }
    for name in spec.target_env.keys() {
        validate_target_env_name(name)?;
    }

    let cwd = canonical_directory(Path::new(&spec.cwd), "sandbox working directory")?;
    let mut writable_roots: Vec<(PathBuf, std::os::fd::OwnedFd)> =
        Vec::with_capacity(spec.writable_roots.len());
    for root in &spec.writable_roots {
        let root = canonical_directory(Path::new(root), "sandbox writable root")?;
        if !writable_roots.iter().any(|(existing, _)| existing == &root) {
            reject_nested_mounts(&root)?;
            let fd = open_directory(&root)?;
            writable_roots.push((root, fd));
        }
    }
    let cwd_fd = writable_roots
        .iter()
        .find(|(root, _)| root == &cwd)
        .map(|(_, fd)| std::os::fd::AsRawFd::as_raw_fd(fd))
        .ok_or("sandbox working directory is not an explicitly writable root")?;
    let target = std::fs::canonicalize(target_arg)
        .map_err(|error| format!("sandbox executable is unavailable: {error}"))?;
    if !target.is_file() {
        return Err("sandbox executable is not a regular file".into());
    }
    let pgid = unsafe { libc::getpgrp() };
    let pid = unsafe { libc::getpid() };
    if pgid != pid {
        return Err("sandbox helper is not the process-group leader".into());
    }
    if unsafe { libc::fchdir(cwd_fd) } != 0 {
        return Err(format!(
            "cannot enter sandbox working directory: {}",
            std::io::Error::last_os_error()
        ));
    }
    apply_resource_limits(spec.limits)?;
    set_no_new_privileges()?;
    apply_landlock(&target, &writable_roots)?;
    install_seccomp_filter()?;

    let mut command = std::process::Command::new(&target);
    command
        .args(spec.target_args)
        .env_clear()
        .envs(spec.target_env)
        .stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let error = std::os::unix::process::CommandExt::exec(&mut command);
    Err(format!("sandbox target exec failed: {error}"))
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
fn run_helper(_args: Vec<std::ffi::OsString>) -> Result<(), String> {
    Err("native tool sandboxing is unsupported on this platform".into())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn read_spec_from_stdin() -> Result<SandboxSpec, String> {
    fn read_exact_fd(fd: libc::c_int, output: &mut [u8]) -> Result<(), String> {
        let mut read = 0;
        while read < output.len() {
            let count =
                unsafe { libc::read(fd, output[read..].as_mut_ptr().cast(), output.len() - read) };
            if count == 0 {
                return Err("sandbox launch specification ended early".into());
            }
            if count < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(format!("cannot read sandbox launch specification: {error}"));
            }
            read += count as usize;
        }
        Ok(())
    }

    let mut header = [0u8; 4];
    read_exact_fd(libc::STDIN_FILENO, &mut header)?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > SPEC_MAX_BYTES {
        return Err("sandbox launch specification length is invalid".into());
    }
    let mut bytes = vec![0u8; length];
    read_exact_fd(libc::STDIN_FILENO, &mut bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("sandbox launch specification is malformed: {error}"))
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn close_inherited_descriptors() -> Result<(), String> {
    let result = unsafe { libc::syscall(libc::SYS_close_range, 3u32, u32::MAX, 0u32) };
    if result != 0 {
        return Err(format!(
            "cannot close inherited sandbox descriptors: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn apply_resource_limits(limits: SandboxLimits) -> Result<(), String> {
    let settings = [
        (libc::RLIMIT_CORE, 0),
        (libc::RLIMIT_CPU, limits.cpu_seconds),
        (
            libc::RLIMIT_AS,
            limits.memory_mib.saturating_mul(1024 * 1024),
        ),
        (
            libc::RLIMIT_FSIZE,
            limits.file_size_mib.saturating_mul(1024 * 1024),
        ),
        (libc::RLIMIT_NOFILE, limits.max_open_files),
    ];
    for (resource, value) in settings {
        let value = value as libc::rlim_t;
        let limit = libc::rlimit {
            rlim_cur: value,
            rlim_max: value,
        };
        if unsafe { libc::setrlimit(resource, &limit) } != 0 {
            return Err(format!(
                "cannot set sandbox resource limit {resource}: {}",
                std::io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn set_no_new_privileges() -> Result<(), String> {
    if unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 {
        return Err(format!(
            "cannot set sandbox no_new_privs: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn open_directory(path: &Path) -> Result<std::os::fd::OwnedFd, String> {
    open_path_fd(path, true)
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn open_path_fd(path: &Path, directory: bool) -> Result<std::os::fd::OwnedFd, String> {
    use std::{ffi::CString, os::fd::FromRawFd, os::unix::ffi::OsStrExt};

    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }
    const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
    const RESOLVE_NO_SYMLINKS: u64 = 0x04;

    let c_path = CString::new(path.as_os_str().as_bytes())
        .map_err(|_| "sandbox path contains a NUL byte")?;
    let flags = libc::O_PATH | libc::O_CLOEXEC | if directory { libc::O_DIRECTORY } else { 0 };
    let how = OpenHow {
        flags: flags as u64,
        mode: 0,
        resolve: RESOLVE_NO_MAGICLINKS | RESOLVE_NO_SYMLINKS,
    };
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            libc::AT_FDCWD,
            c_path.as_ptr(),
            &how as *const OpenHow,
            std::mem::size_of::<OpenHow>(),
        )
    };
    if fd < 0 {
        return Err(format!(
            "cannot open sandbox {} {} without symlink traversal: {}",
            if directory { "directory" } else { "path" },
            path.display(),
            std::io::Error::last_os_error()
        ));
    }
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd as i32) })
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn reject_nested_mounts(root: &Path) -> Result<(), String> {
    let mountinfo = std::fs::read("/proc/self/mountinfo")
        .map_err(|error| format!("cannot inspect mounts for sandbox root: {error}"))?;
    if let Some(mount_point) = nested_mount_in(root, &mountinfo)? {
        return Err(format!(
            "sandbox writable root {} contains nested mount {}",
            root.display(),
            mount_point.display()
        ));
    }
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn nested_mount_in(root: &Path, mountinfo: &[u8]) -> Result<Option<PathBuf>, String> {
    use std::os::unix::ffi::OsStringExt;

    for (line_number, line) in mountinfo.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let mount_point = line
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|field| !field.is_empty())
            .nth(4)
            .ok_or_else(|| format!("malformed /proc/self/mountinfo line {}", line_number + 1))?;
        let mut decoded = Vec::with_capacity(mount_point.len());
        let mut index = 0;
        while index < mount_point.len() {
            if mount_point[index] != b'\\' {
                decoded.push(mount_point[index]);
                index += 1;
                continue;
            }
            let escape = mount_point
                .get(index + 1..index + 4)
                .ok_or_else(|| format!("malformed mount escape on line {}", line_number + 1))?;
            if !escape.iter().all(|byte| (b'0'..=b'7').contains(byte)) || escape[0] > b'3' {
                return Err(format!(
                    "malformed mount escape on line {}",
                    line_number + 1
                ));
            }
            decoded
                .push(((escape[0] - b'0') << 6) | ((escape[1] - b'0') << 3) | (escape[2] - b'0'));
            index += 4;
        }
        let mount_point = PathBuf::from(std::ffi::OsString::from_vec(decoded));
        if mount_point != root && mount_point.starts_with(root) {
            return Ok(Some(mount_point));
        }
    }
    Ok(None)
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn apply_landlock(
    target: &Path,
    writable_roots: &[(PathBuf, std::os::fd::OwnedFd)],
) -> Result<(), String> {
    use std::os::unix::io::{FromRawFd, OwnedFd};

    const LANDLOCK_CREATE_RULESET_VERSION: libc::c_uint = 1;
    const LANDLOCK_RULE_PATH_BENEATH: libc::c_int = 1;
    const LANDLOCK_ACCESS_FS_EXECUTE: u64 = 1 << 0;
    const LANDLOCK_ACCESS_FS_WRITE_FILE: u64 = 1 << 1;
    const LANDLOCK_ACCESS_FS_READ_FILE: u64 = 1 << 2;
    const LANDLOCK_ACCESS_FS_READ_DIR: u64 = 1 << 3;
    const LANDLOCK_ACCESS_FS_REMOVE_DIR: u64 = 1 << 4;
    const LANDLOCK_ACCESS_FS_REMOVE_FILE: u64 = 1 << 5;
    const LANDLOCK_ACCESS_FS_MAKE_DIR: u64 = 1 << 7;
    const LANDLOCK_ACCESS_FS_MAKE_REG: u64 = 1 << 8;
    const LANDLOCK_ACCESS_FS_MAKE_FIFO: u64 = 1 << 10;
    const LANDLOCK_ACCESS_FS_MAKE_SYM: u64 = 1 << 12;
    const LANDLOCK_ACCESS_FS_TRUNCATE: u64 = 1 << 14;
    const LANDLOCK_ACCESS_FS_IOCTL_DEV: u64 = 1 << 15;
    const LANDLOCK_ACCESS_NET_BIND_TCP: u64 = 1 << 0;
    const LANDLOCK_ACCESS_NET_CONNECT_TCP: u64 = 1 << 1;
    const LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET: u64 = 1 << 0;
    const LANDLOCK_SCOPE_SIGNAL: u64 = 1 << 1;

    #[repr(C)]
    struct RulesetAttr {
        handled_access_fs: u64,
        handled_access_net: u64,
        scoped: u64,
    }

    #[repr(C, packed)]
    struct PathBeneathAttr {
        allowed_access: u64,
        parent_fd: i32,
    }

    fn path_fd(path: &Path) -> Result<OwnedFd, String> {
        open_path_fd(path, false)
    }

    fn add_path_rule_fd(
        ruleset: libc::c_int,
        path: &Path,
        parent_fd: libc::c_int,
        allowed_access: u64,
    ) -> Result<(), String> {
        let attr = PathBeneathAttr {
            allowed_access,
            parent_fd,
        };
        let result = unsafe {
            libc::syscall(
                libc::SYS_landlock_add_rule,
                ruleset,
                LANDLOCK_RULE_PATH_BENEATH,
                &attr as *const PathBeneathAttr,
                0u32,
            )
        };
        if result != 0 {
            return Err(format!(
                "cannot add Landlock path rule for {}: {}",
                path.display(),
                std::io::Error::last_os_error()
            ));
        }
        Ok(())
    }

    fn add_path_rule(ruleset: libc::c_int, path: &Path, allowed_access: u64) -> Result<(), String> {
        let fd = path_fd(path)?;
        add_path_rule_fd(
            ruleset,
            path,
            std::os::fd::AsRawFd::as_raw_fd(&fd),
            allowed_access,
        )
    }

    let abi = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            std::ptr::null::<RulesetAttr>(),
            0usize,
            LANDLOCK_CREATE_RULESET_VERSION,
        )
    };
    // ABI 5 adds device-ioctl mediation and ABI 6 adds scoped signal and
    // abstract UNIX socket rules. The seccomp filter is an additional layer,
    // not a reason to silently accept a kernel missing the Landlock contract.
    if abi < 6 {
        return Err(format!(
            "Landlock ABI {} is unavailable; ABI 6 or newer is required",
            abi.max(0)
        ));
    }
    let handled_fs = 0x7fff | LANDLOCK_ACCESS_FS_IOCTL_DEV;
    let attr = RulesetAttr {
        handled_access_fs: handled_fs,
        handled_access_net: LANDLOCK_ACCESS_NET_BIND_TCP | LANDLOCK_ACCESS_NET_CONNECT_TCP,
        scoped: LANDLOCK_SCOPE_SIGNAL | LANDLOCK_SCOPE_ABSTRACT_UNIX_SOCKET,
    };
    let ruleset = unsafe {
        libc::syscall(
            libc::SYS_landlock_create_ruleset,
            &attr as *const RulesetAttr,
            std::mem::size_of::<RulesetAttr>(),
            0u32,
        )
    };
    if ruleset < 0 {
        return Err(format!(
            "cannot create Landlock ruleset: {}",
            std::io::Error::last_os_error()
        ));
    }
    let ruleset = unsafe { OwnedFd::from_raw_fd(ruleset as i32) };
    let fd_flags = unsafe { libc::fcntl(std::os::fd::AsRawFd::as_raw_fd(&ruleset), libc::F_GETFD) };
    if fd_flags < 0
        || unsafe {
            libc::fcntl(
                std::os::fd::AsRawFd::as_raw_fd(&ruleset),
                libc::F_SETFD,
                fd_flags | libc::FD_CLOEXEC,
            )
        } < 0
    {
        return Err(format!(
            "cannot mark Landlock ruleset descriptor close-on-exec: {}",
            std::io::Error::last_os_error()
        ));
    }

    let read_only =
        LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_READ_DIR;
    let mut read_roots = Vec::new();
    for candidate in ["/usr", "/bin", "/sbin", "/lib", "/lib64"] {
        if let Ok(path) = std::fs::canonicalize(candidate)
            && path.is_dir()
            && !read_roots.contains(&path)
        {
            read_roots.push(path);
        }
    }
    if read_roots.is_empty() {
        return Err("no read-only runtime directories are available to Landlock".into());
    }
    for root in read_roots {
        add_path_rule(std::os::fd::AsRawFd::as_raw_fd(&ruleset), &root, read_only)?;
    }

    for (path, rights) in [
        (Path::new("/etc/ld.so.cache"), LANDLOCK_ACCESS_FS_READ_FILE),
        (
            Path::new("/dev/null"),
            LANDLOCK_ACCESS_FS_READ_FILE | LANDLOCK_ACCESS_FS_WRITE_FILE,
        ),
        (Path::new("/dev/urandom"), LANDLOCK_ACCESS_FS_READ_FILE),
    ] {
        if path.exists() {
            add_path_rule(std::os::fd::AsRawFd::as_raw_fd(&ruleset), path, rights)?;
        }
    }

    let target = std::fs::canonicalize(target)
        .map_err(|error| format!("cannot resolve sandbox executable: {error}"))?;
    add_path_rule(
        std::os::fd::AsRawFd::as_raw_fd(&ruleset),
        &target,
        LANDLOCK_ACCESS_FS_EXECUTE | LANDLOCK_ACCESS_FS_READ_FILE,
    )?;

    let writable = LANDLOCK_ACCESS_FS_EXECUTE
        | LANDLOCK_ACCESS_FS_WRITE_FILE
        | LANDLOCK_ACCESS_FS_READ_FILE
        | LANDLOCK_ACCESS_FS_READ_DIR
        | LANDLOCK_ACCESS_FS_REMOVE_DIR
        | LANDLOCK_ACCESS_FS_REMOVE_FILE
        | LANDLOCK_ACCESS_FS_MAKE_DIR
        | LANDLOCK_ACCESS_FS_MAKE_REG
        | LANDLOCK_ACCESS_FS_MAKE_FIFO
        | LANDLOCK_ACCESS_FS_MAKE_SYM
        | LANDLOCK_ACCESS_FS_TRUNCATE;
    for (root, root_fd) in writable_roots {
        add_path_rule_fd(
            std::os::fd::AsRawFd::as_raw_fd(&ruleset),
            root,
            std::os::fd::AsRawFd::as_raw_fd(root_fd),
            writable,
        )?;
    }

    if unsafe {
        libc::syscall(
            libc::SYS_landlock_restrict_self,
            std::os::fd::AsRawFd::as_raw_fd(&ruleset),
            0u32,
        )
    } != 0
    {
        return Err(format!(
            "cannot apply Landlock ruleset: {}",
            std::io::Error::last_os_error()
        ));
    }
    drop(ruleset);
    Ok(())
}

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
))]
fn install_seccomp_filter() -> Result<(), String> {
    const BPF_LD_W_ABS: u16 = 0x20;
    const BPF_JMP_JEQ_K: u16 = 0x15;
    const BPF_JMP_JSET_K: u16 = 0x45;
    const BPF_ALU_AND_K: u16 = 0x54;
    const BPF_RET_K: u16 = 0x06;
    const SECCOMP_RET_KILL_PROCESS: u32 = 0x8000_0000;
    const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;
    const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;

    #[repr(C)]
    struct SockFilter {
        code: u16,
        jt: u8,
        jf: u8,
        k: u32,
    }

    #[repr(C)]
    struct SockFprog {
        len: u16,
        filter: *const SockFilter,
    }

    fn stmt(code: u16, k: u32) -> SockFilter {
        SockFilter {
            code,
            jt: 0,
            jf: 0,
            k,
        }
    }
    fn jump(code: u16, k: u32, jt: u8, jf: u8) -> SockFilter {
        SockFilter { code, jt, jf, k }
    }

    #[cfg(target_arch = "x86_64")]
    const AUDIT_ARCH: u32 = 0xc000_003e;
    #[cfg(target_arch = "aarch64")]
    const AUDIT_ARCH: u32 = 0xc000_00b7;

    let mut filter = vec![
        stmt(BPF_LD_W_ABS, 4), // seccomp_data.arch
        jump(BPF_JMP_JEQ_K, AUDIT_ARCH, 1, 0),
        stmt(BPF_RET_K, SECCOMP_RET_KILL_PROCESS),
    ];
    #[cfg(target_arch = "x86_64")]
    filter.extend([
        stmt(BPF_LD_W_ABS, 0),
        jump(BPF_JMP_JSET_K, 0x4000_0000, 0, 1), // reject the x32 ABI
        stmt(BPF_RET_K, SECCOMP_RET_KILL_PROCESS),
    ]);
    filter.push(stmt(BPF_LD_W_ABS, 0)); // seccomp_data.nr

    let mut deny = vec![
        libc::SYS_socket,
        libc::SYS_socketpair,
        libc::SYS_connect,
        libc::SYS_bind,
        libc::SYS_listen,
        libc::SYS_accept,
        libc::SYS_accept4,
        libc::SYS_sendto,
        libc::SYS_recvfrom,
        libc::SYS_sendmsg,
        libc::SYS_recvmsg,
        libc::SYS_sendmmsg,
        libc::SYS_recvmmsg,
        libc::SYS_shutdown,
        libc::SYS_setsockopt,
        libc::SYS_getsockopt,
        libc::SYS_unshare,
        libc::SYS_setns,
        libc::SYS_mount,
        libc::SYS_umount2,
        libc::SYS_pivot_root,
        libc::SYS_chroot,
        libc::SYS_mount_setattr,
        libc::SYS_open_tree,
        libc::SYS_fsopen,
        libc::SYS_fsconfig,
        libc::SYS_fsmount,
        libc::SYS_move_mount,
        libc::SYS_setsid,
        libc::SYS_setpgid,
        libc::SYS_kill,
        libc::SYS_tkill,
        libc::SYS_tgkill,
        libc::SYS_rt_sigqueueinfo,
        libc::SYS_rt_tgsigqueueinfo,
        libc::SYS_pidfd_send_signal,
        libc::SYS_ptrace,
        libc::SYS_process_vm_readv,
        libc::SYS_process_vm_writev,
        libc::SYS_process_madvise,
        libc::SYS_process_mrelease,
        libc::SYS_kcmp,
        libc::SYS_pidfd_open,
        libc::SYS_pidfd_getfd,
        libc::SYS_prctl,
        libc::SYS_bpf,
        libc::SYS_perf_event_open,
        libc::SYS_userfaultfd,
        libc::SYS_io_uring_setup,
        libc::SYS_io_uring_enter,
        libc::SYS_io_uring_register,
        libc::SYS_keyctl,
        libc::SYS_add_key,
        libc::SYS_request_key,
        libc::SYS_open_by_handle_at,
        libc::SYS_name_to_handle_at,
        libc::SYS_fanotify_init,
        libc::SYS_fanotify_mark,
        libc::SYS_init_module,
        libc::SYS_finit_module,
        libc::SYS_delete_module,
        libc::SYS_kexec_load,
        libc::SYS_kexec_file_load,
        libc::SYS_reboot,
        libc::SYS_swapon,
        libc::SYS_swapoff,
        libc::SYS_syslog,
        libc::SYS_acct,
        libc::SYS_vhangup,
        libc::SYS_setuid,
        libc::SYS_setgid,
        libc::SYS_setreuid,
        libc::SYS_setregid,
        libc::SYS_setresuid,
        libc::SYS_setresgid,
        libc::SYS_setfsuid,
        libc::SYS_setfsgid,
        libc::SYS_setgroups,
        libc::SYS_capset,
        libc::SYS_personality,
        libc::SYS_fchmod,
        libc::SYS_fchmodat,
        libc::SYS_fchown,
        libc::SYS_fchownat,
        libc::SYS_utimensat,
        libc::SYS_setxattr,
        libc::SYS_lsetxattr,
        libc::SYS_fsetxattr,
        libc::SYS_removexattr,
        libc::SYS_lremovexattr,
        libc::SYS_fremovexattr,
        libc::SYS_mknodat,
        libc::SYS_linkat,
        libc::SYS_shmget,
        libc::SYS_shmat,
        libc::SYS_shmctl,
        libc::SYS_semget,
        libc::SYS_semop,
        libc::SYS_semtimedop,
        libc::SYS_semctl,
        libc::SYS_msgget,
        libc::SYS_msgsnd,
        libc::SYS_msgrcv,
        libc::SYS_msgctl,
        libc::SYS_inotify_init1,
        libc::SYS_inotify_add_watch,
        libc::SYS_inotify_rm_watch,
    ];
    #[cfg(target_arch = "x86_64")]
    deny.extend([
        libc::SYS__sysctl,
        libc::SYS_modify_ldt,
        libc::SYS_iopl,
        libc::SYS_ioperm,
        libc::SYS_chmod,
        libc::SYS_fchmodat2,
        libc::SYS_chown,
        libc::SYS_lchown,
        libc::SYS_utime,
        libc::SYS_utimes,
        libc::SYS_futimesat,
        libc::SYS_mknod,
        libc::SYS_link,
        libc::SYS_inotify_init,
    ]);
    // Restrict prlimit64 to the calling process. Changing a resource limit on
    // an unrelated same-UID CCM process is outside the sandbox contract.
    filter.push(jump(BPF_JMP_JEQ_K, libc::SYS_prlimit64 as u32, 0, 3));
    filter.push(stmt(BPF_LD_W_ABS, 16)); // prlimit64 pid
    filter.push(jump(BPF_JMP_JEQ_K, 0, 1, 0));
    filter.push(stmt(BPF_RET_K, SECCOMP_RET_ERRNO | libc::EPERM as u32));
    filter.push(stmt(BPF_LD_W_ABS, 0));
    for syscall in deny {
        filter.push(jump(BPF_JMP_JEQ_K, syscall as u32, 0, 1));
        filter.push(stmt(BPF_RET_K, SECCOMP_RET_ERRNO | libc::EPERM as u32));
    }

    // glibc uses clone3 when present and falls back to clone on ENOSYS. Block
    // clone3 entirely; legacy clone is allowed only without new namespaces or
    // CLONE_PARENT, which would create an untracked child owned by CCM.
    const CLONE_REJECT_FLAGS: u32 = 0x7e02_0080 | libc::CLONE_PARENT as u32;
    filter.push(jump(BPF_JMP_JEQ_K, libc::SYS_clone3 as u32, 0, 1));
    filter.push(stmt(BPF_RET_K, SECCOMP_RET_ERRNO | libc::ENOSYS as u32));
    filter.push(jump(BPF_JMP_JEQ_K, libc::SYS_clone as u32, 0, 4));
    filter.push(stmt(BPF_LD_W_ABS, 16)); // clone flags, low 32 bits
    filter.push(stmt(BPF_ALU_AND_K, CLONE_REJECT_FLAGS));
    filter.push(jump(BPF_JMP_JEQ_K, 0, 1, 0));
    filter.push(stmt(BPF_RET_K, SECCOMP_RET_ERRNO | libc::EPERM as u32));
    filter.push(stmt(BPF_LD_W_ABS, 0));
    filter.push(stmt(BPF_RET_K, SECCOMP_RET_ALLOW));
    if filter.len() > u16::MAX as usize {
        return Err("seccomp filter exceeds kernel instruction limit".into());
    }
    let program = SockFprog {
        len: filter.len() as u16,
        filter: filter.as_ptr(),
    };
    if unsafe {
        libc::prctl(
            libc::PR_SET_SECCOMP,
            libc::SECCOMP_MODE_FILTER,
            &program as *const SockFprog,
            0,
            0,
        )
    } != 0
    {
        return Err(format!(
            "cannot install seccomp filter: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(())
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
fn apply_landlock(_target: &Path, _writable_roots: &[(PathBuf, ())]) -> Result<(), String> {
    Err("Landlock confinement is unsupported on this platform".into())
}

#[cfg(not(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64")
)))]
fn install_seccomp_filter() -> Result<(), String> {
    Err("seccomp confinement is unsupported on this platform".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn sandbox_defaults_and_explicit_overrides_are_platform_specific() {
        assert_eq!(
            decide_sandbox_mode(None, None, SandboxPlatform::Linux).mode,
            SandboxMode::Required
        );
        assert_eq!(
            decide_sandbox_mode(None, None, SandboxPlatform::MacOs).mode,
            SandboxMode::Disabled
        );
        assert_eq!(
            decide_sandbox_mode(Some(false), None, SandboxPlatform::Linux).mode,
            SandboxMode::Disabled
        );
        assert_eq!(
            decide_sandbox_mode(Some(true), None, SandboxPlatform::MacOs).mode,
            SandboxMode::Unsupported
        );
        assert_eq!(
            decide_sandbox_mode(None, Some(false), SandboxPlatform::Linux).reason,
            "mcp_override_disabled"
        );
        assert_eq!(
            decide_sandbox_mode(Some(false), Some(true), SandboxPlatform::Linux).mode,
            SandboxMode::Required
        );
        assert_eq!(
            decide_sandbox_mode(None, None, SandboxPlatform::Unsupported).mode,
            SandboxMode::Unsupported
        );
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[test]
    fn default_tool_config_resolves_to_required_linux_sandbox() {
        let config = crate::config::ToolingConfig::default();
        assert_eq!(config.sandbox_enabled, None);
        assert_eq!(
            resolve_mode(config.sandbox_enabled, None),
            Ok(SandboxMode::Required)
        );
    }

    #[test]
    fn sandbox_config_validation_can_exercise_platform_defaults_without_claiming_runtime_support() {
        let mut config = crate::config::ToolingConfig {
            shell_enabled: true,
            ..Default::default()
        };
        assert!(validate_config_for_platform(&config, SandboxPlatform::Linux).is_ok());
        assert!(validate_config_for_platform(&config, SandboxPlatform::MacOs).is_ok());

        config.sandbox_enabled = Some(true);
        assert!(validate_config_for_platform(&config, SandboxPlatform::MacOs).is_err());
        config.sandbox_enabled = Some(false);
        assert!(validate_config_for_platform(&config, SandboxPlatform::MacOs).is_ok());
        assert!(validate_config_for_platform(&config, SandboxPlatform::Unsupported).is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_platform_default_disables_native_sandbox_and_explicit_enable_fails_closed() {
        let mut config = crate::config::ToolingConfig {
            shell_enabled: true,
            ..Default::default()
        };
        assert_eq!(resolve_mode(None, None), Ok(SandboxMode::Disabled));
        assert!(validate_config(&config).is_ok());

        config.sandbox_enabled = Some(true);
        assert!(resolve_mode(config.sandbox_enabled, None).is_err());
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn sandbox_status_projection_excludes_commands_environment_and_paths() {
        let mut config = crate::config::ToolingConfig {
            sandbox_enabled: Some(false),
            ..Default::default()
        };
        config.mcp_servers.insert(
            "stdio-test".into(),
            crate::config::McpServerConfig {
                profile: crate::config::McpProviderProfile {
                    reviewed: true,
                    reach: crate::config::McpReach::Local,
                    impact: crate::config::McpImpact::Low,
                },
                transport: crate::config::McpTransportConfig::Stdio {
                    command: "/private/provider".into(),
                    args: vec!["secret-arg".into()],
                    env: BTreeMap::from([("TOKEN".into(), "TOKEN_SOURCE".into())]),
                    sandbox: crate::config::McpSandboxConfig {
                        enabled: Some(true),
                        executable: String::new(),
                    },
                },
                tools: BTreeMap::new(),
            },
        );
        let status = runtime_status(&config).to_string();
        assert!(status.contains("mcp_override_enabled"));
        assert!(!status.contains("/private/provider"));
        assert!(!status.contains("secret-arg"));
        assert!(!status.contains("TOKEN_SOURCE"));
    }

    #[test]
    fn environment_allowlist_rejects_dynamic_loader_and_helper_channels() {
        assert!(validate_target_env_name("API_TOKEN").is_ok());
        assert!(validate_target_env_name("LD_PRELOAD").is_err());
        assert!(validate_target_env_name("GLIBC_TUNABLES").is_err());
        assert!(validate_target_env_name("CCM_SANDBOX_SPEC").is_err());
        assert!(validate_target_env_name("BAD=NAME").is_err());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn explicitly_unconfined_child_keeps_environment_and_outside_workspace_access() {
        let root = SandboxTempDir::new().unwrap();
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let outside = root.path().join("outside.txt");
        std::fs::write(&outside, "outside-visible").unwrap();
        let env = BTreeMap::from([
            (
                "VISIBLE_FILE".into(),
                outside.to_string_lossy().into_owned(),
            ),
            ("MCP_ACCESS_TOKEN".into(), "explicit-only".into()),
        ]);
        let process = SandboxCommand::new(
            "/bin/sh",
            &[
                "-c".into(),
                "cat \"$VISIBLE_FILE\"; printf '|%s|%s' \"${HOME-unset}\" \"$MCP_ACCESS_TOKEN\""
                    .into(),
            ],
            &workspace,
            &[workspace.clone()],
            SandboxLimits::default(),
            env,
            SandboxMode::Disabled,
            Stdio::piped(),
            Stdio::null(),
        )
        .unwrap();
        let mut child = process.spawn(false).await.unwrap();
        let mut stdout = child.take_stdout().unwrap();
        let output = read_bounded(&mut stdout, 1024).await.unwrap();
        let status = child.wait_and_cleanup().await.unwrap();
        assert!(status.success());
        assert_eq!(
            String::from_utf8_lossy(&output),
            "outside-visible|unset|explicit-only"
        );
    }

    #[cfg(any(
        all(
            target_os = "linux",
            any(target_arch = "x86_64", target_arch = "aarch64")
        ),
        target_os = "macos"
    ))]
    #[tokio::test]
    async fn explicitly_unconfined_child_closes_inherited_descriptors() {
        use std::os::fd::{AsRawFd, FromRawFd};

        let root = SandboxTempDir::new().unwrap();
        let sentinel = root.path().join("fd-sentinel");
        std::fs::write(&sentinel, "inherited").unwrap();
        let file = std::fs::File::open(sentinel).unwrap();
        let fd = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD, 64) };
        assert!(fd >= 64);
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        let inherited = unsafe { std::fs::File::from_raw_fd(fd) };
        let probe = format!("test ! -e /dev/fd/{fd}");
        let process = SandboxCommand::new(
            "/bin/sh",
            &["-c".into(), probe],
            root.path(),
            &[root.path().to_path_buf()],
            SandboxLimits::default(),
            BTreeMap::new(),
            SandboxMode::Disabled,
            Stdio::piped(),
            Stdio::null(),
        )
        .unwrap();
        let mut child = process.spawn(false).await.unwrap();
        let mut stdout = child.take_stdout().unwrap();
        let _ = read_bounded(&mut stdout, 1024).await.unwrap();
        assert!(child.wait_and_cleanup().await.unwrap().success());
        drop(inherited);
        drop(file);
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn explicitly_unconfined_timeout_still_kills_process_group_descendants() {
        let root = SandboxTempDir::new().unwrap();
        let marker = root.path().join("late-write");
        let script = format!(
            "(sleep 0.3; printf leaked > '{}') & sleep 5",
            marker.display()
        );
        let process = SandboxCommand::new(
            "/bin/sh",
            &["-c".into(), script],
            root.path(),
            &[root.path().to_path_buf()],
            SandboxLimits::default(),
            BTreeMap::new(),
            SandboxMode::Disabled,
            Stdio::piped(),
            Stdio::null(),
        )
        .unwrap();
        let mut child = process.spawn(false).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), child.wait_and_cleanup())
                .await
                .is_err()
        );
        child.kill_and_wait().await;
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(
            !marker.exists(),
            "unconfined descendant survived timeout cleanup"
        );
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn required_sandbox_setup_failure_never_falls_back_to_native_execution() {
        let root = SandboxTempDir::new().unwrap();
        let marker = root.path().join("must-not-exist");
        let process = SandboxCommand::new_with_helper(
            Path::new("/bin/false"),
            "/bin/sh",
            &["-c".into(), format!("touch '{}'", marker.display())],
            root.path(),
            &[root.path().to_path_buf()],
            SandboxLimits::default(),
            BTreeMap::new(),
            Stdio::piped(),
            Stdio::null(),
        )
        .unwrap();
        let mut child = process.spawn(false).await.unwrap();
        assert!(!child.wait_and_cleanup().await.unwrap().success());
        assert!(!marker.exists());
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[test]
    fn mountinfo_parser_rejects_nested_mounts_and_decodes_escaped_paths() {
        let root = Path::new("/tmp/work space");
        let mountinfo = b"24 1 0:1 / /tmp/work\\040space rw,relatime - ext4 /dev/test rw\n";
        assert_eq!(nested_mount_in(root, mountinfo).unwrap(), None);

        let nested =
            b"24 1 0:1 / /tmp/work\\040space/data\\040mount rw,relatime - ext4 /dev/test rw\n";
        assert_eq!(
            nested_mount_in(root, nested).unwrap(),
            Some(PathBuf::from("/tmp/work space/data mount"))
        );
        assert!(nested_mount_in(root, b"malformed\n").is_err());
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[test]
    fn sandbox_syscall_probe() {
        let Ok(mode) = std::env::var("SANDBOX_PROBE_MODE") else {
            return;
        };
        match mode.as_str() {
            "environment" => {
                assert!(std::env::var_os("HOME").is_none());
                assert!(std::env::var_os("OLLAMA_CLOUD_API_KEY").is_none());
                assert_eq!(
                    std::env::var("MCP_ACCESS_TOKEN").as_deref(),
                    Ok("explicit-only")
                );
            }
            "limits" => {
                fn current(resource: libc::__rlimit_resource_t) -> u64 {
                    let mut limit = unsafe { std::mem::zeroed::<libc::rlimit>() };
                    assert_eq!(unsafe { libc::getrlimit(resource, &mut limit) }, 0);
                    limit.rlim_cur as u64
                }
                assert_eq!(current(libc::RLIMIT_CPU), 3);
                assert_eq!(current(libc::RLIMIT_AS), 256 * 1024 * 1024);
                assert_eq!(current(libc::RLIMIT_FSIZE), 2 * 1024 * 1024);
                assert_eq!(current(libc::RLIMIT_NOFILE), 32);
            }
            "syscalls" => {
                let socket = unsafe {
                    libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0)
                };
                assert_eq!(socket, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );
                let unix_socket = unsafe {
                    libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0)
                };
                assert_eq!(unix_socket, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );

                assert_eq!(unsafe { libc::setsid() }, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );
                assert_eq!(unsafe { libc::setpgid(0, 0) }, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );
                assert_eq!(unsafe { libc::kill(libc::getpid(), 0) }, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );

                let clone3 = unsafe {
                    libc::syscall(libc::SYS_clone3, std::ptr::null::<libc::c_void>(), 0usize)
                };
                assert_eq!(clone3, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::ENOSYS)
                );

                let io_uring = unsafe {
                    libc::syscall(
                        libc::SYS_io_uring_setup,
                        2u32,
                        std::ptr::null::<libc::c_void>(),
                    )
                };
                assert_eq!(io_uring, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );

                assert_eq!(
                    unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) },
                    -1
                );
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM)
                );
            }
            "clone_parent" => {
                let flags = (libc::CLONE_PARENT | libc::SIGCHLD) as libc::c_long;
                let result = unsafe {
                    libc::syscall(
                        libc::SYS_clone,
                        flags,
                        std::ptr::null_mut::<libc::c_void>(),
                        0usize,
                        0usize,
                        0usize,
                    )
                };
                if result == 0 {
                    unsafe { libc::_exit(0) };
                }
                if result > 0 {
                    // This only runs if the filter regresses. Let the outer
                    // test process reap the one unexpected direct child.
                    let marker = format!("CLONE_PARENT_CHILD_PID={result}\n");
                    unsafe {
                        libc::write(libc::STDOUT_FILENO, marker.as_ptr().cast(), marker.len());
                    }
                    panic!("CLONE_PARENT unexpectedly created child {result}");
                }
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EPERM),
                    "CLONE_PARENT was not rejected by seccomp"
                );
            }
            #[cfg(target_arch = "x86_64")]
            "x32" => {
                let _ = unsafe { libc::syscall((libc::SYS_getpid as libc::c_long) | 0x4000_0000) };
                panic!("x32 syscall unexpectedly passed the seccomp architecture gate");
            }
            #[cfg(target_arch = "x86_64")]
            "compat" => {
                let mut eax = 20u32; // i386 __NR_getpid
                unsafe {
                    std::arch::asm!("int 0x80", inout("eax") eax, options(nostack));
                }
                panic!(
                    "i386 compat syscall unexpectedly passed the seccomp architecture gate: {eax}"
                );
            }
            "fd" => {
                assert_eq!(unsafe { libc::fcntl(200, libc::F_GETFD) }, -1);
                assert_eq!(
                    std::io::Error::last_os_error().raw_os_error(),
                    Some(libc::EBADF)
                );
                assert!(std::fs::File::open("/proc/self/fd/200").is_err());
                assert!(std::fs::File::open("/dev/fd/200").is_err());
            }
            "refer" => {
                use std::os::unix::ffi::OsStrExt;

                let from = std::ffi::CString::new(
                    Path::new(&std::env::var_os("SANDBOX_RENAME_FROM").unwrap())
                        .as_os_str()
                        .as_bytes(),
                )
                .unwrap();
                let to = std::ffi::CString::new(
                    Path::new(&std::env::var_os("SANDBOX_RENAME_TO").unwrap())
                        .as_os_str()
                        .as_bytes(),
                )
                .unwrap();
                let result = unsafe { libc::rename(from.as_ptr(), to.as_ptr()) };
                assert_eq!(result, -1, "cross-directory rename unexpectedly succeeded");
            }
            "metadata" => {
                use std::os::unix::ffi::OsStrExt;

                let path = std::ffi::CString::new(
                    Path::new(&std::env::var_os("SANDBOX_METADATA_PATH").unwrap())
                        .as_os_str()
                        .as_bytes(),
                )
                .unwrap();
                let mut stat = unsafe { std::mem::zeroed::<libc::stat>() };
                assert_eq!(unsafe { libc::stat(path.as_ptr(), &mut stat) }, 0);
                let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
                assert_eq!(fd, -1, "outside file contents unexpectedly readable");
            }
            "ioctl" => {
                let path = c"/dev/null";
                let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
                assert!(fd >= 0);
                let mut winsize = unsafe { std::mem::zeroed::<libc::winsize>() };
                let result = unsafe { libc::ioctl(fd, libc::TIOCGWINSZ, &mut winsize) };
                let error = std::io::Error::last_os_error();
                unsafe { libc::close(fd) };
                assert_eq!(result, -1);
                assert_eq!(error.raw_os_error(), Some(libc::EACCES));
            }
            _ => panic!("unknown sandbox syscall probe: {mode}"),
        }
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    fn test_helper_binary() -> PathBuf {
        let test_binary = std::env::current_exe().unwrap();
        let helper = test_binary
            .parent()
            .and_then(Path::parent)
            .unwrap()
            .join("ccm");
        assert!(
            helper.is_file(),
            "cargo test must build the regular CCM executable for helper tests: {}",
            helper.display()
        );
        helper
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    fn test_root() -> (SandboxTempDir, PathBuf) {
        use std::os::unix::fs::DirBuilderExt;

        let base = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let root = (0..4)
            .find_map(|_| {
                let path = base.join(format!("ccm-sandbox-test-{}", uuid::Uuid::new_v4()));
                let mut builder = std::fs::DirBuilder::new();
                builder.mode(0o700);
                match builder.create(&path) {
                    Ok(()) => Some(SandboxTempDir { path }),
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => None,
                    Err(error) => panic!("cannot create sandbox test fixture: {error}"),
                }
            })
            .expect("cannot allocate sandbox test fixture");
        let workspace = root.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        (root, workspace)
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    fn test_command(
        target: &str,
        args: &[String],
        cwd: &Path,
        env: BTreeMap<String, String>,
    ) -> SandboxCommand {
        SandboxCommand::new_with_helper(
            &test_helper_binary(),
            target,
            args,
            cwd,
            &[cwd.to_path_buf()],
            SandboxLimits {
                cpu_seconds: 3,
                memory_mib: 256,
                file_size_mib: 2,
                max_open_files: 32,
                max_processes: 1,
            },
            env,
            Stdio::piped(),
            Stdio::null(),
        )
        .unwrap()
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    async fn test_run(
        process: SandboxCommand,
        max_output: usize,
        timeout: Duration,
    ) -> Result<(std::process::ExitStatus, Vec<u8>), String> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut child = tokio::time::timeout_at(deadline, process.spawn(false))
            .await
            .map_err(|_| "test sandbox spawn timeout")??;
        let mut stdout = child.take_stdout().ok_or("test stdout unavailable")?;
        let supervised = tokio::time::timeout_at(deadline, async {
            let output = read_bounded(&mut stdout, max_output);
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
        match supervised {
            Ok(result) => {
                if result.is_err() {
                    child.kill_and_wait().await;
                }
                result
            }
            Err(_) => {
                child.kill_and_wait().await;
                Err("test sandbox timed out".into())
            }
        }
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    async fn run_probe(
        workspace: &Path,
        mode: &str,
        mut env: BTreeMap<String, String>,
    ) -> Result<(std::process::ExitStatus, Vec<u8>), String> {
        env.insert("SANDBOX_PROBE_MODE".into(), mode.into());
        if mode == "environment" {
            env.insert("MCP_ACCESS_TOKEN".into(), "explicit-only".into());
        }
        let test_exe = std::env::current_exe().map_err(|error| error.to_string())?;
        let args = vec![
            "sandbox_syscall_probe".into(),
            "--nocapture".into(),
            "--test-threads=1".into(),
        ];
        test_run(
            test_command(
                test_exe.to_str().ok_or("test binary path is not UTF-8")?,
                &args,
                workspace,
                env,
            ),
            16 * 1024,
            Duration::from_secs(5),
        )
        .await
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    async fn run_clone_parent_probe(workspace: &Path) {
        let (status, output) = run_probe(workspace, "clone_parent", BTreeMap::new())
            .await
            .unwrap();
        let output_text = String::from_utf8_lossy(&output);
        if let Some(pid) = output_text
            .lines()
            .find_map(|line| line.strip_prefix("CLONE_PARENT_CHILD_PID="))
            .and_then(|pid| pid.parse::<libc::pid_t>().ok())
        {
            let mut child_status = 0;
            let reaped = unsafe { libc::waitpid(pid, &mut child_status, 0) };
            assert_eq!(reaped, pid, "reap the single unexpected CLONE_PARENT child");
        }
        assert!(status.success(), "CLONE_PARENT probe failed: {output_text}");
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    async fn run_architecture_probe(workspace: &Path, mode: &str) {
        use std::os::unix::process::ExitStatusExt;

        let env = BTreeMap::from([("SANDBOX_PROBE_MODE".into(), mode.into())]);
        let test_exe = std::env::current_exe().unwrap();
        let process = test_command(
            test_exe.to_str().unwrap(),
            &[
                "sandbox_syscall_probe".into(),
                "--nocapture".into(),
                "--test-threads=1".into(),
            ],
            workspace,
            env,
        );
        let (status, _) = test_run(process, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(
            status.signal(),
            Some(libc::SIGSYS),
            "{mode} syscall was not killed by the seccomp architecture gate"
        );
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn shared_launcher_confines_filesystem_environment_fds_and_syscalls() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        assert_eq!(resolve_mode(None, None), Ok(SandboxMode::Required));
        let (root, workspace) = test_root();
        let outside = root.path().join("outside.txt");
        std::fs::write(&outside, "original").unwrap();
        symlink(&outside, workspace.join("escape")).unwrap();
        std::fs::create_dir(workspace.join("a")).unwrap();
        std::fs::create_dir(workspace.join("b")).unwrap();
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();

        let env = BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("TMPDIR".into(), workspace.to_string_lossy().into_owned()),
            ("MCP_ACCESS_TOKEN".into(), "explicit-only".into()),
        ]);
        let shell = test_command(
            "/bin/sh",
            &[
                "-c".into(),
                "printf safe > allowed.txt; cat allowed.txt; printf '|%s|%s|%s' \"${HOME-unset}\" \"${OLLAMA_CLOUD_API_KEY-unset}\" \"$MCP_ACCESS_TOKEN\"".into(),
            ],
            &workspace,
            env,
        );
        let (status, output) = test_run(shell, 1024, Duration::from_secs(5)).await.unwrap();
        assert!(status.success(), "shell helper failed: {status}");
        assert_eq!(
            String::from_utf8_lossy(&output),
            "safe|unset|unset|explicit-only"
        );
        assert_eq!(
            std::fs::read_to_string(workspace.join("allowed.txt")).unwrap(),
            "safe"
        );

        let outside_path = outside.to_string_lossy();
        let denied_read = test_command(
            "/bin/sh",
            &["-c".into(), format!("cat '{}'", outside_path)],
            &workspace,
            BTreeMap::new(),
        );
        let (status, output) = test_run(denied_read, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!status.success());
        assert!(output.is_empty());

        let denied_write = test_command(
            "/bin/sh",
            &["-c".into(), format!("printf changed > '{}'", outside_path)],
            &workspace,
            BTreeMap::new(),
        );
        let (status, _) = test_run(denied_write, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!status.success());
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "original");

        let symlink_write = test_command(
            "/bin/sh",
            &["-c".into(), "printf changed > escape".into()],
            &workspace,
            BTreeMap::new(),
        );
        let (status, _) = test_run(symlink_write, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!status.success());
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "original");

        let rename_from = workspace.join("refer-source");
        let rename_to = workspace.join("a/refer-target");
        std::fs::write(&rename_from, "remain here").unwrap();
        let probe = run_probe(
            &workspace,
            "refer",
            BTreeMap::from([
                (
                    "SANDBOX_RENAME_FROM".into(),
                    rename_from.to_string_lossy().into_owned(),
                ),
                (
                    "SANDBOX_RENAME_TO".into(),
                    rename_to.to_string_lossy().into_owned(),
                ),
            ]),
        )
        .await
        .unwrap();
        assert!(
            probe.0.success(),
            "cross-directory REFER probe failed: {}",
            String::from_utf8_lossy(&probe.1)
        );
        assert!(rename_from.exists());
        assert!(!rename_to.exists());

        for mode in ["environment", "limits", "syscalls", "ioctl"] {
            let probe = run_probe(&workspace, mode, BTreeMap::new()).await.unwrap();
            assert!(
                probe.0.success(),
                "{mode} probe failed: {}",
                String::from_utf8_lossy(&probe.1)
            );
        }
        run_clone_parent_probe(&workspace).await;
        assert!(native_sandbox_supported());

        let metadata_probe = run_probe(
            &workspace,
            "metadata",
            BTreeMap::from([(
                "SANDBOX_METADATA_PATH".into(),
                outside.to_string_lossy().into_owned(),
            )]),
        )
        .await
        .unwrap();
        assert!(
            metadata_probe.0.success(),
            "metadata boundary probe failed: {}",
            String::from_utf8_lossy(&metadata_probe.1)
        );

        #[cfg(target_arch = "x86_64")]
        for mode in ["x32", "compat"] {
            run_architecture_probe(&workspace, mode).await;
        }
        drop(root);
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn stdio_profile_writes_only_to_private_scratch_not_workspace() {
        let (root, workspace) = test_root();
        let scratch = root.path().join("mcp-scratch");
        std::fs::create_dir(&scratch).unwrap();
        let denied = workspace.join("provider-write");
        let env = BTreeMap::from([
            ("PATH".into(), "/usr/bin:/bin".into()),
            ("TMPDIR".into(), scratch.to_string_lossy().into_owned()),
        ]);
        let process = test_command(
            "/bin/sh",
            &[
                "-c".into(),
                format!(
                    "printf scratch > \"$TMPDIR/result\"; printf workspace > '{}'",
                    denied.display()
                ),
            ],
            &scratch,
            env,
        );
        let (status, _) = test_run(process, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(!status.success());
        assert_eq!(
            std::fs::read_to_string(scratch.join("result")).unwrap(),
            "scratch"
        );
        assert!(
            !denied.exists(),
            "stdio provider gained workspace write access"
        );
        drop(root);
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn inherited_non_cloexec_descriptors_are_closed_before_exec() {
        use std::{
            os::fd::{AsRawFd, FromRawFd},
            os::unix::fs::PermissionsExt,
        };

        let (root, workspace) = test_root();
        let sentinel = root.path().join("fd-sentinel");
        std::fs::write(&sentinel, "dummy descriptor").unwrap();
        let file = std::fs::File::open(&sentinel).unwrap();
        let fd = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD, 200) };
        assert!(fd >= 200);
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        assert!(flags >= 0);
        assert_eq!(
            unsafe { libc::fcntl(fd, libc::F_SETFD, flags & !libc::FD_CLOEXEC) },
            0
        );
        let inherited = unsafe { std::fs::File::from_raw_fd(fd) };
        std::fs::set_permissions(&workspace, std::fs::Permissions::from_mode(0o700)).unwrap();
        let probe = run_probe(&workspace, "fd", BTreeMap::new()).await.unwrap();
        assert!(
            probe.0.success(),
            "fd probe failed: {}",
            String::from_utf8_lossy(&probe.1)
        );
        drop(inherited);
        drop(file);
        drop(root);
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn cleanup_kills_descendants_on_timeout_normal_exit_and_future_drop() {
        let (root, workspace) = test_root();
        let marker = workspace.join("late-write");
        let delayed = format!(
            "(sleep 0.3; printf leaked > '{}') & sleep 5",
            marker.display()
        );
        let timeout_process = test_command(
            "/bin/sh",
            &["-c".into(), delayed.clone()],
            &workspace,
            BTreeMap::new(),
        );
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        let mut child = tokio::time::timeout_at(deadline, timeout_process.spawn(false))
            .await
            .unwrap()
            .unwrap();
        let mut stdout = child.take_stdout().unwrap();
        let timed = tokio::time::timeout_at(deadline, read_bounded(&mut stdout, 1024)).await;
        if timed.is_err() {
            child.kill_and_wait().await;
        } else {
            let _ = child.wait_and_cleanup().await;
        }
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(!marker.exists(), "timed-out descendant wrote after cleanup");

        let normal_exit = format!(
            "(sleep 0.3; printf leaked > '{}') & exit 0",
            marker.display()
        );
        let process = test_command(
            "/bin/sh",
            &["-c".into(), normal_exit],
            &workspace,
            BTreeMap::new(),
        );
        let (status, _) = test_run(process, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(status.success());
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(!marker.exists(), "leader exit left a live descendant");

        let drop_process = test_command(
            "/bin/sh",
            &["-c".into(), delayed],
            &workspace,
            BTreeMap::new(),
        );
        let future = tokio::spawn(async move {
            let mut child = drop_process.spawn(false).await.unwrap();
            let mut stdout = child.take_stdout().unwrap();
            let _ = read_bounded(&mut stdout, 1024).await;
            child.wait_and_cleanup().await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        future.abort();
        let _ = future.await;
        tokio::time::sleep(Duration::from_millis(450)).await;
        assert!(
            !marker.exists(),
            "dropped invocation left a live descendant"
        );
        drop(root);
    }

    #[cfg(all(
        target_os = "linux",
        any(target_arch = "x86_64", target_arch = "aarch64")
    ))]
    #[tokio::test]
    async fn workspace_hardlink_alias_is_an_explicit_trust_boundary() {
        let (root, workspace) = test_root();
        let outside = root.path().join("outside-hardlink");
        std::fs::write(&outside, "before").unwrap();
        std::fs::hard_link(&outside, workspace.join("alias")).unwrap();
        let process = test_command(
            "/bin/sh",
            &["-c".into(), "printf after > alias".into()],
            &workspace,
            BTreeMap::new(),
        );
        let (status, _) = test_run(process, 1024, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(status.success());
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "after",
            "Landlock workspace rules do not distinguish a pre-existing outside hardlink"
        );
        drop(root);
    }

    #[test]
    fn unsupported_platform_is_explicitly_not_available() {
        if !native_sandbox_supported() {
            assert!(SandboxTempDir::new().is_err());
        }
    }
}
