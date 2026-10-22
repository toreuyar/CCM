use std::net::SocketAddr;
use std::sync::{OnceLock, RwLock};

static TOOLING_CONFIG: OnceLock<RwLock<ToolingConfig>> = OnceLock::new();

/// Process-wide tool integration config. `Config::from_env` installs the validated
/// configuration before serving requests; existing AppState constructors remain
/// source-compatible while tool modules share this immutable-at-runtime snapshot.
pub fn tooling_config() -> ToolingConfig {
    TOOLING_CONFIG
        .get_or_init(|| RwLock::new(ToolingConfig::default()))
        .read()
        .expect("tooling config lock poisoned")
        .clone()
}

fn install_tooling_config(config: ToolingConfig) {
    *TOOLING_CONFIG
        .get_or_init(|| RwLock::new(ToolingConfig::default()))
        .write()
        .expect("tooling config lock poisoned") = config;
}

#[cfg(test)]
pub(crate) fn set_test_tooling_config(config: ToolingConfig) {
    install_tooling_config(config);
}

#[cfg(test)]
static TOOLING_CONFIG_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

#[cfg(test)]
pub(crate) struct ToolingConfigTestGuard {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    original: ToolingConfig,
}

#[cfg(test)]
impl Drop for ToolingConfigTestGuard {
    fn drop(&mut self) {
        install_tooling_config(self.original.clone());
    }
}

#[cfg(test)]
pub(crate) async fn lock_tooling_config_for_test() -> ToolingConfigTestGuard {
    let lock = TOOLING_CONFIG_TEST_LOCK.get_or_init(|| tokio::sync::Mutex::new(()));
    let guard = lock.lock().await;
    ToolingConfigTestGuard {
        _lock: guard,
        original: tooling_config(),
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolPermission {
    Allow,
    Ask,
    #[default]
    Deny,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpReach {
    Local,
    PrivateNetwork,
    PublicNetwork,
    Unrestricted,
    #[default]
    Unreviewed,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum McpImpact {
    Low,
    Medium,
    High,
    #[default]
    Unreviewed,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ToolGrant {
    pub tool: String,
    pub decision: ToolPermission,
    pub agent_id: Option<String>,
    pub stage_id: Option<String>,
}

impl Default for ToolGrant {
    fn default() -> Self {
        Self {
            tool: String::new(),
            decision: ToolPermission::Deny,
            agent_id: None,
            stage_id: None,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkforceToolPolicy {
    pub default: ToolPermission,
    pub grants: Vec<ToolGrant>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpProviderProfile {
    pub reviewed: bool,
    pub reach: McpReach,
    pub impact: McpImpact,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpToolConfig {
    pub description: String,
    pub input_schema: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct McpSandboxConfig {
    /// Optional per-server override of `tools.sandbox_enabled`. Omission keeps
    /// the platform default; explicit false requests an unconfined child.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,
    /// Legacy setting retained for config compatibility. CCM's app-owned
    /// sandbox is used; this value is no longer executed.
    pub executable: String,
}

impl Default for McpSandboxConfig {
    fn default() -> Self {
        Self {
            enabled: None,
            executable: String::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SandboxLimits {
    pub cpu_seconds: u64,
    pub memory_mib: u64,
    pub file_size_mib: u64,
    pub max_open_files: u64,
    /// Legacy compatibility field; currently ignored. RLIMIT_NPROC is UID-wide
    /// and is not applied as if it were a per-invocation process quota.
    pub max_processes: u64,
}

impl Default for SandboxLimits {
    fn default() -> Self {
        Self {
            cpu_seconds: 10,
            memory_mib: 512,
            file_size_mib: 64,
            max_open_files: 64,
            max_processes: 16,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "transport", rename_all = "snake_case", deny_unknown_fields)]
pub enum McpTransportConfig {
    Stdio {
        command: String,
        #[serde(default)]
        args: Vec<String>,
        #[serde(default)]
        env: std::collections::BTreeMap<String, String>,
        #[serde(default)]
        sandbox: McpSandboxConfig,
    },
    StreamableHttp {
        url: String,
        #[serde(default)]
        bearer_token_env: Option<String>,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerConfig {
    pub profile: McpProviderProfile,
    pub transport: McpTransportConfig,
    /// Exact tool-name allowlist. Each schema is administrator-supplied and bounded.
    pub tools: std::collections::BTreeMap<String, McpToolConfig>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ToolingConfig {
    /// Relative paths are resolved against the configuration file's directory.
    pub plugin_dir: String,
    pub workspace_root: String,
    pub max_input_bytes: usize,
    pub max_result_bytes: usize,
    pub max_schema_bytes: usize,
    pub max_total_schema_bytes: usize,
    pub max_tools: usize,
    pub wasm_max_bytes: usize,
    pub wasm_memory_bytes: usize,
    pub wasm_fuel: u64,
    pub request_timeout_ms: u64,
    /// Explicit opt-in for the embedded shell tool. Workforce grants still
    /// default to deny. `shell_sandbox_executable` is a legacy opt-in alias.
    pub shell_enabled: bool,
    /// Legacy Bubblewrap setting retained as an opt-in alias; the executable is
    /// never invoked. New configurations should use `shell_enabled`.
    pub shell_sandbox_executable: Option<String>,
    /// Shared native-child sandbox setting. Omission selects the platform
    /// default (required on supported Linux, disabled on macOS); false is an
    /// explicit request to run native children without sandbox isolation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sandbox_enabled: Option<bool>,
    pub sandbox_limits: SandboxLimits,
    /// Exact host allowlist for the embedded generic HTTP tool.
    pub http_allowed_hosts: Vec<String>,
    /// Missing workforce policy, and every missing grant, is deny.
    pub workforces: std::collections::BTreeMap<String, WorkforceToolPolicy>,
    pub mcp_servers: std::collections::BTreeMap<String, McpServerConfig>,
}

impl Default for ToolingConfig {
    fn default() -> Self {
        Self {
            plugin_dir: "plugins".into(),
            workspace_root: ".".into(),
            max_input_bytes: 16 * 1024,
            max_result_bytes: 16 * 1024,
            max_schema_bytes: 8 * 1024,
            max_total_schema_bytes: 32 * 1024,
            max_tools: 64,
            wasm_max_bytes: 2 * 1024 * 1024,
            wasm_memory_bytes: 16 * 1024 * 1024,
            wasm_fuel: 1_000_000,
            request_timeout_ms: 10_000,
            shell_enabled: false,
            shell_sandbox_executable: None,
            sandbox_enabled: None,
            sandbox_limits: SandboxLimits::default(),
            http_allowed_hosts: Vec::new(),
            workforces: Default::default(),
            mcp_servers: Default::default(),
        }
    }
}

impl ToolingConfig {
    fn resolve_paths(&mut self, base: &std::path::Path) {
        for path in [&mut self.plugin_dir, &mut self.workspace_root] {
            let value = std::path::Path::new(path);
            if value.is_relative() {
                *path = base.join(value).to_string_lossy().into_owned();
            }
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        if !(1024..=1024 * 1024).contains(&self.max_input_bytes)
            || !(1024..=1024 * 1024).contains(&self.max_result_bytes)
            || !(256..=64 * 1024).contains(&self.max_schema_bytes)
            || !(self.max_schema_bytes..=512 * 1024).contains(&self.max_total_schema_bytes)
            || !(4..=128).contains(&self.max_tools)
            || !(1024..=16 * 1024 * 1024).contains(&self.wasm_max_bytes)
            || !(64 * 1024..=256 * 1024 * 1024).contains(&self.wasm_memory_bytes)
            || !(1_000..=100_000_000).contains(&self.wasm_fuel)
            || !(100..=120_000).contains(&self.request_timeout_ms)
            || !(1..=120).contains(&self.sandbox_limits.cpu_seconds)
            || !(64..=4096).contains(&self.sandbox_limits.memory_mib)
            || !(1..=1024).contains(&self.sandbox_limits.file_size_mib)
            || !(16..=1024).contains(&self.sandbox_limits.max_open_files)
        {
            return Err("tool limits are outside supported bounds".into());
        }
        if self.plugin_dir.trim().is_empty() || self.workspace_root.trim().is_empty() {
            return Err("tools.plugin_dir and tools.workspace_root must not be empty".into());
        }
        if self
            .shell_sandbox_executable
            .as_deref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("tools.shell_sandbox_executable must not be empty".into());
        }
        crate::runtime::tools::validate_sandbox_config(self)?;
        for host in &self.http_allowed_hosts {
            if host.trim().is_empty()
                || host
                    .chars()
                    .any(|c| c.is_whitespace() || matches!(c, '@' | '*' | '?' | '#'))
                || host.trim() != host
            {
                return Err(format!("invalid tools.http_allowed_hosts entry: {host}"));
            }
            let address = if host.contains("://") {
                host.clone()
            } else {
                format!("https://{host}")
            };
            let parsed = reqwest::Url::parse(&address)
                .map_err(|_| format!("invalid tools.http_allowed_hosts entry: {host}"))?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.path() != "/"
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                return Err(format!("invalid tools.http_allowed_hosts entry: {host}"));
            }
        }
        for (workforce, policy) in &self.workforces {
            if workforce.trim().is_empty() {
                return Err("tools.workforces keys must not be empty".into());
            }
            let mut seen = std::collections::HashSet::new();
            for grant in &policy.grants {
                if grant.tool.trim().is_empty()
                    || grant.tool.contains('*')
                    || grant
                        .stage_id
                        .as_ref()
                        .is_some_and(|stage| stage.trim().is_empty())
                    || grant
                        .agent_id
                        .as_ref()
                        .is_some_and(|agent| agent.trim().is_empty())
                    || (grant.stage_id.is_some() && grant.agent_id.is_none())
                {
                    return Err(format!("invalid tool grant for workforce {workforce}"));
                }
                let key = (
                    grant.agent_id.as_deref(),
                    grant.stage_id.as_deref(),
                    grant.tool.as_str(),
                );
                if !seen.insert(key) {
                    return Err(format!("duplicate tool grant for workforce {workforce}"));
                }
            }
        }
        for (server_id, server) in &self.mcp_servers {
            if !valid_config_name(server_id) {
                return Err(format!("invalid MCP server id: {server_id}"));
            }
            if !server.profile.reviewed
                || server.profile.reach == McpReach::Unreviewed
                || server.profile.impact == McpImpact::Unreviewed
            {
                return Err(format!(
                    "MCP server {server_id} requires a reviewed reach/impact profile"
                ));
            }
            if server.tools.len() > self.max_tools {
                return Err(format!("MCP server {server_id} exceeds tools.max_tools"));
            }
            let mut total_schema_bytes = 0usize;
            for (tool_name, tool) in &server.tools {
                if !valid_config_name(tool_name) || tool.description.len() > 2048 {
                    return Err(format!(
                        "invalid MCP tool declaration {server_id}.{tool_name}"
                    ));
                }
                let bytes = serde_json::to_vec(&tool.input_schema)
                    .map_err(|e| format!("invalid MCP schema: {e}"))?;
                if bytes.len() > self.max_schema_bytes {
                    return Err(format!(
                        "MCP schema {server_id}.{tool_name} exceeds size limit"
                    ));
                }
                total_schema_bytes = total_schema_bytes.saturating_add(bytes.len());
                crate::runtime::tools::validate_schema(&tool.input_schema)?;
            }
            if total_schema_bytes > self.max_total_schema_bytes {
                return Err(format!(
                    "MCP schemas for {server_id} exceed total size limit"
                ));
            }
            match &server.transport {
                McpTransportConfig::Stdio {
                    command,
                    args,
                    env,
                    sandbox: _,
                } => {
                    if !std::path::Path::new(command).is_absolute() {
                        return Err(format!(
                            "MCP stdio command for {server_id} must be absolute"
                        ));
                    }
                    if args.len() > 128
                        || args.iter().map(String::len).sum::<usize>() > 64 * 1024
                        || env.len() > 64
                    {
                        return Err(format!(
                            "MCP stdio server {server_id} exceeds sandbox argument/environment limits"
                        ));
                    }
                    for (name, source) in env {
                        if !valid_sandbox_env_name(name) || !valid_env_name(source) {
                            return Err(format!(
                                "invalid environment mapping for MCP server {server_id}"
                            ));
                        }
                    }
                }
                McpTransportConfig::StreamableHttp {
                    url,
                    bearer_token_env,
                } => {
                    let parsed = reqwest::Url::parse(url)
                        .map_err(|_| format!("invalid Streamable HTTP URL for {server_id}"))?;
                    if !matches!(parsed.scheme(), "http" | "https")
                        || parsed.host_str().is_none()
                        || !parsed.username().is_empty()
                        || parsed.password().is_some()
                        || parsed.fragment().is_some()
                        || bearer_token_env
                            .as_ref()
                            .is_some_and(|name| !valid_env_name(name))
                    {
                        return Err(format!(
                            "invalid Streamable HTTP URL or token environment for {server_id}"
                        ));
                    }
                }
            }
        }
        Ok(())
    }
}

fn valid_config_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 24
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn valid_env_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn valid_sandbox_env_name(name: &str) -> bool {
    valid_env_name(name)
        && !name.starts_with("LD_")
        && !name.starts_with("CCM_SANDBOX_")
        && !matches!(
            name,
            "GLIBC_TUNABLES" | "GCONV_PATH" | "LOCPATH" | "MALLOC_TRACE"
        )
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub api_key: String,
    pub upstream_base_url: String,
    pub model: String,
    pub host: String,
    pub port: u16,
    pub upstream: UpstreamConfig,
    pub portfolio_db: String,
    pub max_iterations: u32,
    pub debug_enabled: bool,
    pub creation: CreationConfig,
    pub improvement: crate::improvement::ImprovementConfig,
    pub personality_transparency: PersonalityTransparency,
    pub memory: MemoryConfig,
    #[serde(default)]
    pub shutdown: ShutdownConfig,
    #[serde(default)]
    pub routing: crate::model_config::Routing,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PersonalityTransparency {
    #[default]
    Introvert,
    Extrovert,
    Flexible,
}

#[derive(Debug, Default)]
pub struct ServerConfig {
    pub creation: CreationConfig,
    pub improvement: crate::improvement::ImprovementConfig,
    pub tools: ToolingConfig,
    pub personality_transparency: PersonalityTransparency,
    pub memory: MemoryConfig,
    pub shutdown: ShutdownConfig,
    pub routing: crate::model_config::Routing,
    pub operational: crate::model_config::Overrides,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct MemoryConfig {
    pub budget_mib: usize,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self { budget_mib: 256 }
    }
}

impl MemoryConfig {
    pub fn bytes(&self) -> usize {
        self.budget_mib * 1024 * 1024
    }
}

/// Bounded process-shutdown budgets. The HTTP grace period covers open response
/// connections, the async drain covers accepted work and its background workers,
/// and cleanup covers durable publication plus the Store/traffic fence. The exit
/// margin keeps the application's own deadline inside the Supervisor escalation
/// threshold when that threshold is the configured one. These are implementation
/// defaults selected for this project, not a claim about an external container
/// or host termination grace.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ShutdownConfig {
    pub http_grace_secs: u64,
    pub async_drain_secs: u64,
    pub cleanup_secs: u64,
    pub exit_margin_secs: u64,
    pub supervisor_stopwait_secs: u64,
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            http_grace_secs: 5,
            async_drain_secs: 120,
            cleanup_secs: 60,
            exit_margin_secs: 10,
            supervisor_stopwait_secs: 300,
        }
    }
}

impl ShutdownConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.http_grace_secs == 0
            || self.async_drain_secs == 0
            || self.cleanup_secs == 0
            || self.supervisor_stopwait_secs == 0
        {
            return Err(
                "shutdown grace, drain, cleanup and supervisor_stopwait_secs must be positive"
                    .into(),
            );
        }
        let total = self
            .http_grace_secs
            .saturating_add(self.async_drain_secs)
            .saturating_add(self.cleanup_secs)
            .saturating_add(self.exit_margin_secs);
        if total > self.supervisor_stopwait_secs {
            return Err(format!(
                "shutdown budgets ({total}s including the exit margin) exceed supervisor_stopwait_secs ({})",
                self.supervisor_stopwait_secs
            ));
        }
        Ok(())
    }

    pub fn http_grace(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.http_grace_secs)
    }

    pub fn async_drain(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.async_drain_secs)
    }

    pub fn cleanup(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.cleanup_secs)
    }
}

/// Independent provider budgets; total covers retries and their backoff too.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UpstreamConfig {
    pub connect_timeout_ms: u64,
    pub first_response_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub total_timeout_ms: u64,
    pub max_attempts: u32,
    pub retry_base_ms: u64,
}

impl Default for UpstreamConfig {
    fn default() -> Self {
        Self {
            connect_timeout_ms: 10_000,
            first_response_timeout_ms: 60_000,
            idle_timeout_ms: 60_000,
            total_timeout_ms: 1_800_000,
            max_attempts: 3,
            retry_base_ms: 500,
        }
    }
}

impl UpstreamConfig {
    fn from_env() -> Result<Self, String> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let mut c = Self::default();
        for (name, target) in [
            ("CCM_UPSTREAM_CONNECT_TIMEOUT_MS", &mut c.connect_timeout_ms),
            (
                "CCM_UPSTREAM_FIRST_RESPONSE_TIMEOUT_MS",
                &mut c.first_response_timeout_ms,
            ),
            ("CCM_UPSTREAM_IDLE_TIMEOUT_MS", &mut c.idle_timeout_ms),
            ("CCM_UPSTREAM_TOTAL_TIMEOUT_MS", &mut c.total_timeout_ms),
            ("CCM_UPSTREAM_RETRY_BASE_MS", &mut c.retry_base_ms),
        ] {
            if let Some(value) = get(name) {
                *target = value
                    .parse::<u64>()
                    .ok()
                    .filter(|v| (1..=86_400_000).contains(v))
                    .ok_or_else(|| format!("{name} must be between 1 and 86400000 ms"))?;
            }
        }
        if let Some(value) = get("CCM_UPSTREAM_MAX_ATTEMPTS") {
            c.max_attempts = value
                .parse::<u32>()
                .ok()
                .filter(|v| (1..=10).contains(v))
                .ok_or("CCM_UPSTREAM_MAX_ATTEMPTS must be between 1 and 10")?;
        }
        Ok(c)
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CreationConfig {
    pub policy: crate::policy::CreationPolicy,
    pub prompt: String,
    pub consolidation_prompt: String,
}

impl Default for CreationConfig {
    fn default() -> Self {
        Self {
            policy: crate::policy::CreationPolicy::Dynamic,
            prompt: include_str!("../prompts/creation.md").into(),
            consolidation_prompt: include_str!("../prompts/consolidation.md").into(),
        }
    }
}

impl CreationConfig {
    #[cfg(test)]
    pub fn load(path: &std::path::Path, required: bool) -> Result<Self, String> {
        Ok(ServerConfig::load(path, required)?.creation)
    }
}

impl ServerConfig {
    pub fn load(path: &std::path::Path, required: bool) -> Result<Self, String> {
        #[derive(serde::Deserialize, Default)]
        #[serde(deny_unknown_fields)]
        struct File {
            #[serde(default)]
            creation_policy: crate::policy::CreationPolicy,
            #[serde(default)]
            improvement: crate::improvement::ImprovementConfig,
            #[serde(default)]
            tools: ToolingConfig,
            creation_prompt: Option<String>,
            consolidation_prompt: Option<String>,
            #[serde(default)]
            personality_transparency: PersonalityTransparency,
            #[serde(default)]
            memory: MemoryConfig,
            #[serde(default)]
            shutdown: ShutdownConfig,
            #[serde(default)]
            providers: std::collections::BTreeMap<String, crate::model_config::Provider>,
            #[serde(default)]
            models: std::collections::BTreeMap<String, crate::model_config::Model>,
            default_model: Option<String>,
            #[serde(default)]
            agents: std::collections::BTreeMap<String, crate::model_config::Overrides>,
            #[serde(default)]
            upstream: crate::model_config::Timeouts,
            max_iterations: Option<u32>,
            debug_enabled: Option<bool>,
            provider: Option<String>,
            inference_model: Option<String>,
            #[serde(default)]
            agent_models: std::collections::BTreeMap<String, crate::model_config::Candidate>,
            #[serde(default)]
            generated_agent_default: crate::model_config::Assignment,
        }
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(e) if !required && e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(e) => return Err(format!("{}: {e}", path.display())),
        };
        let file: File =
            serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        file.improvement.validate()?;
        let mut tools = file.tools;
        tools.resolve_paths(path.parent().unwrap_or(std::path::Path::new(".")));
        tools.validate()?;
        if !(32..=16384).contains(&file.memory.budget_mib) {
            return Err("memory.budget_mib must be between 32 and 16384".into());
        }
        file.shutdown.validate()?;
        let mut config = CreationConfig {
            policy: file.creation_policy,
            ..CreationConfig::default()
        };
        let load = |name: String| -> Result<String, String> {
            let path = path
                .parent()
                .unwrap_or(std::path::Path::new("."))
                .join(name);
            let text =
                std::fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            if text.trim().is_empty() {
                return Err(format!("empty prompt file {}", path.display()));
            }
            Ok(text)
        };
        if let Some(name) = file.creation_prompt {
            config.prompt = load(name)?;
        }
        if let Some(name) = file.consolidation_prompt {
            config.consolidation_prompt = load(name)?;
        }
        Ok(Self {
            creation: config,
            improvement: file.improvement,
            tools,
            personality_transparency: file.personality_transparency,
            memory: file.memory,
            shutdown: file.shutdown,
            operational: crate::model_config::Overrides {
                upstream: file.upstream,
                max_iterations: file.max_iterations,
                debug_enabled: file.debug_enabled,
                provider: file.provider,
                model: file.inference_model,
                ..Default::default()
            },
            routing: crate::model_config::Routing {
                providers: file.providers,
                models: file.models,
                default_model: file.default_model.unwrap_or_default(),
                agents: file.agents,
                agent_models: file.agent_models,
                generated_agent_default: file.generated_agent_default,
                ..Default::default()
            }
            .load_prompts(path)?,
        })
    }
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        let api_key = std::env::var("OLLAMA_CLOUD_API_KEY").unwrap_or_default();

        let upstream_base_url = std::env::var("CCM_UPSTREAM_BASE_URL")
            .unwrap_or_else(|_| "https://ollama.com/v1".to_string())
            .trim_end_matches('/')
            .to_string();

        let model =
            std::env::var("CCM_MODEL").unwrap_or_else(|_| "deepseek-v4.1-flash".to_string());

        let host = std::env::var("CCM_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

        let port = match std::env::var("CCM_PORT") {
            Ok(p) => p
                .parse::<u16>()
                .map_err(|_| format!("invalid CCM_PORT: {p}"))?,
            Err(_) => 8080,
        };

        let upstream = UpstreamConfig::from_env()?;
        if std::env::var_os("CCM_EVALUATION_TIMEOUT_MS").is_some() {
            tracing::warn!(
                "CCM_EVALUATION_TIMEOUT_MS is obsolete and ignored; use CCM_UPSTREAM_* timeout settings"
            );
        }

        let portfolio_db =
            std::env::var("CCM_PORTFOLIO_DB").unwrap_or_else(|_| "portfolio.db".to_string());

        let max_iterations = match std::env::var("CCM_MAX_ITERATIONS") {
            Ok(v) => v
                .parse::<u32>()
                .map_err(|_| format!("invalid CCM_MAX_ITERATIONS: {v}"))?,
            Err(_) => 2,
        };

        let debug_enabled = match std::env::var("CCM_DEBUG") {
            Ok(v) => crate::telemetry::parse_flag(&v),
            Err(_) => false,
        };
        let path = std::env::var("CCM_CONFIG").ok();
        let server = ServerConfig::load(
            std::path::Path::new(path.as_deref().unwrap_or("ccm.json")),
            path.is_some(),
        )?;
        install_tooling_config(server.tools.clone());

        let mut config = Self {
            api_key,
            upstream_base_url,
            model,
            host,
            port,
            upstream,
            portfolio_db,
            max_iterations,
            debug_enabled,
            creation: server.creation,
            improvement: server.improvement,
            personality_transparency: server.personality_transparency,
            memory: server.memory,
            shutdown: server.shutdown,
            routing: server.routing,
        };
        server.operational.apply(&mut config)?;
        config.initialize_models()?;
        Ok(config)
    }

    pub fn addr(&self) -> SocketAddr {
        format!("{}:{}", self.host, self.port)
            .parse()
            .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], self.port)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_budgets_ignore_legacy_cap_and_validate_overrides() {
        let config = UpstreamConfig::from_lookup(|name| {
            (name == "CCM_EVALUATION_TIMEOUT_MS").then(|| "30000".into())
        })
        .unwrap();
        assert_eq!(config.total_timeout_ms, 1_800_000);
        assert_eq!(config.idle_timeout_ms, 60_000);
        assert_eq!(config.max_attempts, 3);
        for invalid in ["0", "-1", "oops", "86400001"] {
            assert!(
                UpstreamConfig::from_lookup(
                    |name| (name == "CCM_UPSTREAM_TOTAL_TIMEOUT_MS").then(|| invalid.into())
                )
                .is_err()
            );
        }
        assert!(
            UpstreamConfig::from_lookup(
                |name| (name == "CCM_UPSTREAM_MAX_ATTEMPTS").then(|| "11".into())
            )
            .is_err()
        );
        let config = UpstreamConfig::from_lookup(|name| {
            (name == "CCM_UPSTREAM_IDLE_TIMEOUT_MS").then(|| "90000".into())
        })
        .unwrap();
        assert_eq!(config.idle_timeout_ms, 90_000);
    }

    #[test]
    fn memory_budget_is_configurable_bounded_and_defaults_to_256_mib() {
        let path = std::env::temp_dir().join(format!("ccm-config-{}.json", uuid::Uuid::new_v4()));
        for (text, expected) in [("{}", 256), (r#"{"memory":{"budget_mib":64}}"#, 64)] {
            std::fs::write(&path, text).unwrap();
            assert_eq!(
                ServerConfig::load(&path, true).unwrap().memory.budget_mib,
                expected
            );
        }
        for text in [
            r#"{"memory":{"budget_mib":0}}"#,
            r#"{"memory":{"budget_mib":16385}}"#,
            r#"{"memory":{"budget_mib":-1}}"#,
            r#"{"memory":{"unknown":1}}"#,
        ] {
            std::fs::write(&path, text).unwrap();
            assert!(ServerConfig::load(&path, true).is_err());
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn shutdown_budgets_are_bounded_and_fit_inside_the_supervisor_threshold() {
        let defaults = ShutdownConfig::default();
        defaults.validate().unwrap();
        assert_eq!(defaults.http_grace(), std::time::Duration::from_secs(5));
        assert_eq!(defaults.async_drain(), std::time::Duration::from_secs(120));
        assert_eq!(defaults.cleanup(), std::time::Duration::from_secs(60));

        let too_long = ShutdownConfig {
            async_drain_secs: 240,
            ..ShutdownConfig::default()
        };
        assert!(too_long.validate().is_err());
        let zero = ShutdownConfig {
            cleanup_secs: 0,
            ..ShutdownConfig::default()
        };
        assert!(zero.validate().is_err());
        let path = std::env::temp_dir().join(format!("ccm-shutdown-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(
            &path,
            r#"{"shutdown":{"async_drain_secs":10,"cleanup_secs":20}}"#,
        )
        .unwrap();
        let loaded = ServerConfig::load(&path, true).unwrap();
        assert_eq!(loaded.shutdown.async_drain_secs, 10);
        assert_eq!(loaded.shutdown.cleanup_secs, 20);
        assert_eq!(loaded.shutdown.supervisor_stopwait_secs, 300);
        std::fs::write(
            &path,
            r#"{"shutdown":{"async_drain_secs":200,"cleanup_secs":100}}"#,
        )
        .unwrap();
        assert!(ServerConfig::load(&path, true).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn tooling_configuration_is_backward_compatible_bounded_and_requires_reviewed_mcp_profiles() {
        let path =
            std::env::temp_dir().join(format!("ccm-tools-config-{}.json", uuid::Uuid::new_v4()));
        std::fs::write(&path, "{}").unwrap();
        let default = ServerConfig::load(&path, true).unwrap();
        assert_eq!(default.tools.max_result_bytes, 16 * 1024);
        assert!(default.tools.mcp_servers.is_empty());

        let unreviewed = r#"{
            "tools":{"mcp_servers":{"provider":{
                "profile":{"reviewed":false,"reach":"local","impact":"high"},
                "transport":{"transport":"streamable_http","url":"http://127.0.0.1:9999/mcp"},
                "tools":{}
            }}}
        }"#;
        std::fs::write(&path, unreviewed).unwrap();
        assert!(ServerConfig::load(&path, true).is_err());

        let oversized = r#"{"tools":{"max_result_bytes":1048577}}"#;
        std::fs::write(&path, oversized).unwrap();
        assert!(ServerConfig::load(&path, true).is_err());
        let invalid_sandbox =
            r#"{"tools":{"shell_sandbox_executable":"/bin/sh","sandbox_limits":{"memory_mib":0}}}"#;
        std::fs::write(&path, invalid_sandbox).unwrap();
        assert!(ServerConfig::load(&path, true).is_err());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn sandbox_config_distinguishes_omission_from_explicit_disable() {
        let defaults: ToolingConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(defaults.sandbox_enabled, None);
        assert!(
            serde_json::to_value(&defaults)
                .unwrap()
                .get("sandbox_enabled")
                .is_none()
        );

        let explicitly_disabled: ToolingConfig =
            serde_json::from_str(r#"{"sandbox_enabled":false}"#).unwrap();
        assert_eq!(explicitly_disabled.sandbox_enabled, Some(false));

        let omitted_mcp_setting: McpSandboxConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(omitted_mcp_setting.enabled, None);
        assert!(
            serde_json::to_value(&omitted_mcp_setting)
                .unwrap()
                .get("enabled")
                .is_none()
        );

        let explicit_mcp_disable: McpSandboxConfig =
            serde_json::from_str(r#"{"enabled":false}"#).unwrap();
        assert_eq!(explicit_mcp_disable.enabled, Some(false));
    }

    #[test]
    fn addr_defaults_to_localhost() {
        let config = Config {
            routing: Default::default(),
            api_key: "k".into(),
            upstream_base_url: "https://ollama.com/v1".into(),
            model: "deepseek-v4.1-flash".into(),
            host: "127.0.0.1".into(),
            port: 8080,
            upstream: UpstreamConfig::default(),
            portfolio_db: "portfolio.db".into(),
            max_iterations: 2,
            debug_enabled: false,
            creation: CreationConfig::default(),
            improvement: crate::improvement::ImprovementConfig::default(),
            personality_transparency: PersonalityTransparency::default(),
            memory: MemoryConfig::default(),
            shutdown: ShutdownConfig::default(),
        };
        assert_eq!(config.addr().to_string(), "127.0.0.1:8080");
    }
}
