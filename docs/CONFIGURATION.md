# Configuration reference

CCM configuration has three layers: process environment, a startup JSON file,
and permitted runtime/client overrides. Credentials belong in environment
variables or protected local files, never committed JSON examples.

## Loading, paths and precedence

1. On startup, dotenv loading fills variables not already present in the process
   environment. Run from your configuration directory; `.env` discovery can also
   find a parent-directory file. Existing exported variables take precedence.
2. `CCM_CONFIG` explicitly selects the JSON file. Otherwise CCM tries `ccm.json`
   in the working directory. A missing implicit file uses built-in defaults;
   a missing explicitly selected file is an error.
3. Unknown JSON fields, invalid values and unavailable referenced prompt files
   fail startup. JSON must be valid JSON, without comments or trailing commas.
4. Model operational overrides take precedence over global JSON settings, then
   supported environment defaults, then built-ins. Nested timeout fields inherit
   individually. Per-agent routing overlays are described below.
5. Saved client creation/personality preferences have their documented scope and
   authority; editing file defaults does not erase saved preferences.

JSON is loaded at startup, not watched. Prompt paths and `tools.plugin_dir` /
`tools.workspace_root` resolve relative to the JSON file's directory. Database
and traffic paths resolve relative to the process working directory. Setting
`CCM_CONFIG` alone does not relocate those files or load a sibling `.env`.

Start with [ccm.example.json](../ccm.example.json). Copy it to ignored `ccm.json`.
Other complete examples are [local Ollama](../examples/ccm.local-provider.json)
and [multiple providers](../examples/ccm.multiple-providers.json). Example model
names must be checked against your installed/provider models before inference.

## Environment variables

| Variable | Built-in default | Purpose |
|---|---|---|
| `CCM_BOOTSTRAP_ADMIN_KEY` | unset | Enables protected APIs and seeds the administrator key. Generate a unique random secret. Without it, `/v1`, `/api` and `/mcp` return 503. |
| `CCM_CONFIG` | implicit `ccm.json` | Startup JSON path. |
| `CCM_HOST` | `127.0.0.1` | Listener address. |
| `CCM_PORT` | `8080` | Listener port, parsed as an unsigned 16-bit integer. |
| `CCM_PORTFOLIO_DB` | `portfolio.db` | Persistent state path; one writer only. |
| `CCM_TRAFFIC_LOG` | `ccm-traffic.jsonl` | Append-only full-traffic journal; parent directory must exist. |
| `OLLAMA_CLOUD_API_KEY` | empty | Credential for the implicit default provider or an explicit `api_key_env` reference. Not universally required for local/other providers. |
| `CCM_UPSTREAM_BASE_URL` | `https://ollama.com/v1` | Connection URL when no explicit provider map is configured. |
| `CCM_MODEL` | `deepseek-v4.1-flash` | Legacy default model/alias; explicit JSON routing normally replaces it. |
| `CCM_MAX_ITERATIONS` | `2` | Client-tool round budget; use a positive integer. |
| `CCM_DEBUG` | `false` | Enables exact-match diagnostic commands, including destructive `debug-delete`; normally leave disabled. |
| `CCM_UPSTREAM_CONNECT_TIMEOUT_MS` | `10000` | Connection deadline. |
| `CCM_UPSTREAM_FIRST_RESPONSE_TIMEOUT_MS` | `60000` | First-response deadline. |
| `CCM_UPSTREAM_IDLE_TIMEOUT_MS` | `60000` | Maximum idle gap while receiving upstream data. |
| `CCM_UPSTREAM_TOTAL_TIMEOUT_MS` | `1800000` | Total provider-call deadline including retry/backoff; maximum 30 minutes. |
| `CCM_UPSTREAM_MAX_ATTEMPTS` | `3` | Total attempts, including the first; environment accepts 1–10. |
| `CCM_UPSTREAM_RETRY_BASE_MS` | `500` | Initial retry backoff. |
| `RUST_LOG` | logging-library fallback | Tracing filter; example: `ccm=info,tower_http=info`. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | unset | Optional best-effort OTLP/HTTP JSON trace destination. |

`CCM_EVALUATION_TIMEOUT_MS` is obsolete and ignored with a warning. Provider
timeouts apply across roles rather than multiplying evaluation-only timeouts.
Environment millisecond overrides parse within 1–86400000; resolved total calls
are still capped at 1800000 ms. Use the values above as a starting point, then
tune according to provider latency and [retry semantics](UPSTREAM_RELIABILITY.md).

## Top-level JSON fields

These are the accepted fields; listener, database path, traffic path and bootstrap
credential are process environment settings, not JSON fields.

| Field | Default / meaning |
|---|---|
| `providers` | Named OpenAI-compatible connections; see below. Empty selects the environment-based default connection. |
| `models` | Public model aliases and per-model settings. Empty falls back to the environment model. |
| `default_model` | Public alias used when a request omits `model`; required with multiple aliases unless the environment default is one of them. |
| `provider` | Global named provider. Can be inferred when there is one provider or a provider named `default`. |
| `inference_model` | Global actual inference ID; distinct from the public alias. |
| `agents` | Per-role/specialist inference overrides. Default `{}`. |
| `agent_models` | Assignment catalog for generated/revised agents, not the public model list. Default `{}`. |
| `generated_agent_default` | Optional `provider` / `model` defaults for new definitions. Default `{}` (inherit). |
| `personality_transparency` | `introvert` (default), `extrovert`, or `flexible`. |
| `creation_policy` | `Dynamic` by default; five exact case-sensitive enum values below. |
| `creation_prompt` | Optional nonempty UTF-8 file; otherwise embedded `prompts/creation.md`. |
| `consolidation_prompt` | Optional nonempty UTF-8 file; otherwise embedded `prompts/consolidation.md`. |
| `max_iterations` | Positive tool-round override; otherwise environment/default. |
| `debug_enabled` | Boolean override of `CCM_DEBUG`. |
| `upstream` | Field-wise provider timeout overrides. |
| `improvement` | Conversation checkpoint settings. |
| `memory` | Cache/buffer allocation budget. |
| `shutdown` | Grace, drain and cleanup budgets. |
| `tools` | Embedded tools, plugins, workforce grants and MCP-client settings. |

## Providers and model aliases

```json
{
  "provider": "cloud",
  "default_model": "my-assistant",
  "providers": {
    "cloud": {
      "base_url": "https://api.openai.com/v1",
      "api_key_env": "OPENAI_API_KEY",
      "structured_output": false,
      "max_concurrent_requests": 2,
      "recovery": { "cooldown_ms": 30000, "max_cooldown_ms": 300000 }
    }
  },
  "models": {
    "my-assistant": { "provider": "cloud", "model": "YOUR_AVAILABLE_MODEL_ID" }
  }
}
```

Replace the model placeholder and set `OPENAI_API_KEY` in your local environment.
This is a protocol/configuration example, not provider-specific acceptance proof.

Provider fields:

- `base_url`: required HTTP(S) OpenAI-compatible API base. No embedded user/password,
  query string or fragment. CCM appends the chat-completions path.
- `api_key_env`: environment variable name; omit for an unauthenticated local
  provider. Missing required credentials fail explicitly, not by switching provider.
- `structured_output`: default `false`. Enable only for a provider verified to
  enforce the Foreman JSON Schema contract; see [routing](FOREMAN_ROUTING.md).
- `max_concurrent_requests`: omitted means unlimited; positive semaphore-capacity
  bound when supplied. Shared across foreground/background calls to that provider.
- `recovery.cooldown_ms`: default 30000. `max_cooldown_ms`: default 300000.
  Require `0 < cooldown_ms <= max_cooldown_ms <= 86400000`.

The `models` map key is the client-visible alias. Its `model` field is the actual
provider model ID. Unknown client aliases are rejected. Model entries support
`provider`, `model`, `personality_transparency`, `creation_policy`, both prompt
paths, `max_iterations`, `debug_enabled`, `upstream`, `agents`, `agent_models`
and `generated_agent_default`. Native web chat uses the default public model.

`upstream` accepts `connect_timeout_ms`, `first_response_timeout_ms`,
`idle_timeout_ms`, `total_timeout_ms`, `max_attempts`, `retry_base_ms`.
JSON call/deadline/attempt values must be positive; JSON backoff may be zero.
The total remains capped at 30 minutes. These are provider-call budgets, not
guarantees about whole-workflow wall time.

### Per-agent overrides and assignment

`agents` selectors are specialist UUIDs/names or built-in IDs `foreman`,
`agent-generator`, `workflow-generator`, `evaluator`. Supported per-agent fields
are `provider`, `model` and `upstream`; request-wide settings are rejected here.
UUID selectors win over names within a layer. Model-specific entries overlay
global entries. Explicit administrator overrides win over generated assignment;
otherwise stored assignment or the selected public model supplies inference.

`agent_models` entries require `provider`, actual `model`, positive
`context_tokens`, nonempty `capabilities` and nonempty `reasoning`. This metadata
is administrator guidance, not verified performance or a tokenizer-enforced
context limit. A model-specific catalog replaces the global catalog. See
[model configuration](MODEL_CONFIGURATION.md) for complete assignment examples
and continuation snapshot behavior.

## Personality and creation policy

- `introvert`: final answer/client tools; internal progress remains in traces.
- `extrovert`: eligible narration/thinking is exposed according to output contracts.
- `flexible`: starts introverted and accepts explicit conversational switches for
  that client/model from the next request. Fixed modes do not accept switches.

Creation policies are `AlwaysPreExecution`, `DefaultPreExecution`, `Dynamic`,
`DefaultPostExecution`, `AlwaysPostExecution`. Always modes are locked; default
modes permit explicit persisted client timing changes; Dynamic lets Foreman choose.
They apply to both agents and workflows. These controls do not grant tool or
workforce permissions. See [creation policy](CREATION_POLICY.md) and
[personality](PERSONALITY_TRANSPARENCY.md).

## Improvement checkpoints

```json
{
  "improvement": {
    "turns": { "enabled": true, "initial_interval": 10,
               "minimum_interval": 5, "maximum_interval": 30 },
    "elapsed": { "enabled": true, "interval_ms": 3600000 },
    "foreground_tokens": { "enabled": false, "threshold": null }
  }
}
```

These are defaults. Turn intervals must be ordered within 5–30; elapsed interval
must be 1–31536000000 ms. Enabled token thresholds must be positive when present;
an absent threshold keeps token triggering disabled. Missing usage is unknown,
not zero. Disable all three triggers to disable discretionary checkpoint triggers;
this does not disable explicit, task-required or policy-owned work.
Permissions and single-key-private eligibility separately control automatic
application. See [improvement](AUTO_IMPROVEMENT.md).

## Memory and shutdown

`memory.budget_mib` defaults to 256, accepts 32–16384, and bounds shared caches
and buffers, **not total process RSS**. SQL commits become visible in memory
before asynchronous publication to disk; a crash may lose unpublished work.

| `shutdown` field | Default seconds |
|---|---:|
| `http_grace_secs` | 5 |
| `async_drain_secs` | 120 |
| `cleanup_secs` | 60 |
| `exit_margin_secs` | 10 |
| `supervisor_stopwait_secs` | 300 |

All except the exit margin must be positive. The first four values must sum to
no more than `supervisor_stopwait_secs`. This validation does not configure your
OS service manager; set its stop timeout separately. See
[deployment](DEPLOYMENT.md) and [memory/persistence](MEMORY_PIPELINE.md).

## Tools, plugins and outbound MCP

Omitting `tools` uses the following defaults. Embedded tool execution is still
subject to workforce grants; a missing workforce/grant defaults to deny.

| `tools` field | Default | Accepted bounds / meaning |
|---|---:|---|
| `plugin_dir` | `plugins` | Nonempty path relative to JSON directory. |
| `workspace_root` | `.` | Nonempty workspace path relative to JSON directory. |
| `max_input_bytes` | 16384 | 1024–1048576 |
| `max_result_bytes` | 16384 | 1024–1048576 |
| `max_schema_bytes` | 8192 | 256–65536 |
| `max_total_schema_bytes` | 32768 | At least `max_schema_bytes`, at most 524288. |
| `max_tools` | 64 | 4–128 |
| `wasm_max_bytes` | 2097152 | 1024–16777216 |
| `wasm_memory_bytes` | 16777216 | 65536–268435456 |
| `wasm_fuel` | 1000000 | 1000–100000000 |
| `request_timeout_ms` | 10000 | 100–120000 |
| `shell_enabled` | `false` | Explicit shell opt-in; grants still apply. |
| `sandbox_enabled` | omitted | Required on supported Linux; disabled on macOS; unsupported native targets fail validation. |
| `http_allowed_hosts` | `[]` | Exact scheme/host/port allowlist, no wildcards or embedded credentials. Scheme omission means HTTPS. |
| `workforces` | `{}` | Policies indexed by actual workforce ID. |
| `mcp_servers` | `{}` | Reviewed outbound MCP configurations. |

`sandbox_limits`: `cpu_seconds=10` (1–120), `memory_mib=512` (64–4096),
`file_size_mib=64` (1–1024), `max_open_files=64` (16–1024).
`max_processes=16` is a retained but **ignored** compatibility field, not a process
quota. `shell_sandbox_executable` is a compatibility shell opt-in; its executable
is not run. New configs should use `shell_enabled`.

Workforce policies contain `default` (`allow`, `ask`, `deny`) and `grants`.
Each grant has `tool`, `decision`, optional `agent_id`, optional `stage_id`.
A stage requires an agent. Stage grants override agent grants, then workforce
tool grants, then workforce default. Provider-visible names such as
`ccm_file_read` differ from grant IDs such as `file.read`.

```json
{
  "tools": {
    "workspace_root": "workspace",
    "workforces": {
      "YOUR_WORKFORCE_ID": {
        "default": "deny",
        "grants": [{ "tool": "file.read", "decision": "allow" }]
      }
    }
  }
}
```

Create the workspace directory and replace the workforce placeholder with an
actual ID. `ask` creates a one-time, key-owned approval rather than a permanent
grant. A model's declared capability does not grant execution authority.

### Outbound MCP server entries

Each `tools.mcp_servers.<id>` has:

- `profile`: `reviewed` boolean, `reach` (`local`, `private_network`,
  `public_network`, `unrestricted`, `unreviewed`) and `impact`
  (`low`, `medium`, `high`, `unreviewed`). These are administrator assessments,
  not automatic isolation. Configuration validates combinations.
- `transport`: tagged object. `transport: "stdio"` uses `command`, optional
  `args`, an explicit `env` map and optional `sandbox` override. `sandbox.enabled`
  overlays the global/platform setting; its legacy `executable` is not executed.
- Alternatively, `transport: "streamable_http"` uses `url` and optional
  `bearer_token_env` referencing a credential environment variable.
- `tools`: exact tool-name allowlist, each with `description` and `input_schema`.
  Wildcards and unbounded/unsupported schemas are rejected.

Keep any credential-bearing stdio `env` map only in ignored local configuration.
For reviewed examples, WASM ABI/manifest fields and precise sandbox boundaries,
see [tools and MCP clients](TOOLS_AND_MCP.md). Inbound `/mcp` is a separate API
protected by CCM bearer keys; see [MCP server](MCP_SERVER.md).

## Runtime settings, diagnostics and sensitive data

Async server/workforce capacity is managed through authenticated admin APIs and
the dashboard. These limits are in-memory and are **not** fields in `ccm.json`;
see [operations](OPERATIONS.md). Model-facing settings are snapshotted for tool
continuations, preventing a continuation from silently switching model identity.

Use `/health/live`, `/health/ready` and authenticated
`GET /api/admin/service/status` to distinguish readiness, draining and persistence
health. Configuration projections redact credentials, but raw prompt/response
payloads, tool inputs and traffic can still contain sensitive user content.
There is no general payload-secret scrubber or built-in journal rotation.

Do not place secrets in URLs, GitHub Actions logs, command-line arguments or public
issues. `.env*`, common key formats, runtime data, local JSON and client-key files
are ignored by this repository; `.env.example` intentionally contains no values.
