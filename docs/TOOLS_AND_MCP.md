# Embedded tools and configured MCP clients

CCM supplies filesystem, shell and generic HTTP capabilities as embedded tools;
they are not exposed as MCP servers by default. Consumer plugins are installed as
WASM files plus JSON manifests, so adding a plugin does not require rebuilding the
CCM binary. Configured MCP clients are separate and support stdio and Streamable
HTTP.

## Permission model

Tool policy is configured in the optional top-level `tools` object in `ccm.json`.
Omitting it is backward-compatible. A missing workforce policy is `deny`; each
configured workforce has a `default` of `allow`, `ask` or `deny` (default `deny`).
Exact tool grants may be workforce-wide, agent-specific, or agent-and-stage-specific.
Stage grants take precedence over agent grants, which take precedence over
workforce-wide exact-tool grants and the workforce default.

Provider-facing embedded-tool candidates are filtered by the resolved workforce,
agent and stage policy: `deny` tools are not advertised, while `allow` and `ask`
tools may be advertised. Runtime policy is checked again before any operation or
approval side effect. This policy filtering is separate from Foreman's optional
invocation-specific subset, which may further narrow the eligible set.

Tool identifiers used in grants are:

| Capability | Grant identifier |
| --- | --- |
| Workspace file read/write | `file.read`, `file.write` |
| Isolated shell | `shell.run` |
| Generic HTTP | `http.request` |
| Installed plugin | `plugin.<manifest-name>` |
| Configured MCP tool | `mcp.<server-id>.<tool-name>` |

Example:

```json
{
  "tools": {
    "plugin_dir": "plugins",
    "workspace_root": "workspace",
    "http_allowed_hosts": ["api.example.test"],
    "shell_enabled": true,
    "workforces": {
      "workforce-id": {
        "default": "deny",
        "grants": [
          {"agent_id": "agent-id", "tool": "file.read", "decision": "allow"},
          {"agent_id": "agent-id", "stage_id": "review", "tool": "mcp.docs.search", "decision": "ask"}
        ]
      }
    }
  }
}
```

`allow` executes after schema and boundary checks. `deny` returns a blocked-tool
result and does not perform that operation. `ask` pauses the internal tool loop before
any side effect and creates a one-time approval handle bound to the exact tool,
arguments, agent, stage, trace run, workforce and invoking API key. A separate
approval action resumes that same paused model/workflow; it never writes an `allow`
grant or otherwise changes the stored policy. If the current policy becomes `deny`,
the target definition changes, or the saved arguments do not match at resume time,
CCM fails closed.

Approval is deliberately separate from OpenAI-compatible client `tool_calls`.
Returning or supplying a client tool call/result cannot approve an embedded CCM
operation. Approval endpoints accept only the handle and the decision in the URL;
they do not accept replacement tool names or arguments.

### Approval handle and response modes

Approval handles include `status_url`, `approve_url`, `deny_url`, `result_url`, and
`cancel_url`, along with the exact operation and invoker scope. Use the **same bearer
key** that initiated the request for status, approval/denial and result access; a
different key in the same workforce is not the same invoker. `GET` requires
`roster_use`; `POST .../approve` and `POST .../deny` require `roster_use`; cancelling
with `DELETE` requires `roster_use` as well. Cancellation is restricted to the
current invoker's exact pending approval and paused request; it does not grant
general async-operation or roster mutation authority.

For a synchronous non-streaming chat request, the response is `202 Accepted` with
`status: "waiting_approval"` and an `approvals` array. This is not a completion and
does not claim that the tool ran. Submit one vote per returned handle, then poll its
result URL. The paused request continues in CCM's process and the result URL returns
the resumed chat completion once available. A batch with multiple `ask` operations
waits for every approval/denial before resuming that batch; no operation from the
batch is performed while a vote is pending.

For `stream: true`, CCM sends a separate SSE event named `ccm.tool_approval`
containing the approval handles. It is not an assistant `tool_calls` event. The stream
remains open while the invoker votes and then carries the resumed output. Explicit
async requests keep their existing `/api/operations/{id}` handle: operation status
becomes `waiting_approval`, exposes `pending_approvals`, and returns to running after
the batch is resolved. The approval handle identifies that same operation; continue
polling the original operation handle for completion/result. Existing async budgets
and worker limits still apply while approval is pending.

The embedded web chat's synchronous `/api/chat` transport returns its normal
completion envelope with an assistant message whose content is a JSON
`waiting_approval` object and the same handles. This is a truthful pending result,
not a completed answer; use the authenticated approval/result endpoints above.

The authenticated MCP server at `POST /mcp` exposes the same pause/resume flow as
`tool_approvals.status`, `.approve`, `.deny`, `.cancel`, and `.result`; each tool is
available to `roster_use` keys and checks the current session and invoker. MCP
`foreman.chat` and `workforce.run_agent`/`workforce.run_workflow` return a structured
`status: "waiting_approval"` response with the handle rather than blocking or
presenting it as an assistant answer. Resolve the handle with one vote, then poll
`tool_approvals.result` for the same resumed synchronous run. Async handles keep
their original operation ID and can be read through `operations.status` and
`operations.result`; async approval results are not cloned into a second
approval-result surface. MCP input bodies are capped at 2 MiB, approval IDs at 128
characters, and tool results at 1 MiB.

`POST .../approve` and `POST .../deny` are one-shot and replay-protected. Denial
resumes the model with a blocked-operation result. `DELETE` cancels the paused request
(and the async operation, when present).

Approval handles, decisions, and approval-flow records are now persisted and flushed
before externally visible state changes are acknowledged. While the request is live,
the existing handle expiry remains bounded by the configured handoff TTL (24 hours by
default) and, for async work, its remaining active-time budget. Restart recovery never
replays an approved tool operation: a pending approval becomes interrupted/not
executed, while an approval already granted for a nonterminal async operation is
reported with an unknown post-restart execution outcome. The caller must make a new
request; a durable approval record does not retain or resume an executable future.
Approval discovery remains scoped to the exact originating key/workforce, and an
interrupted result request returns HTTP 409. The restored-handoff expiry/capacity and
exact-owner held-run-gate findings from review `20261021T205208909024000` were closed
on a frozen source snapshot by follow-up review `20261021T212613516483000`; 14 focused
handoff tests passed. This is source-level acceptance for those fixes, not full
integrated verification or deployment. Shutdown-budget/quiescence/join behavior and
backend handoff remain separate open integration work. See [OPERATIONS.md](OPERATIONS.md) and
[TEST_RESULTS.md](TEST_RESULTS.md) for the verified boundary.

## Built-in boundaries

- `file.read` and `file.write` are rooted at `workspace_root`; absolute and escaping
  paths are rejected. On Linux, file access walks directory descriptors with
  `openat`/`O_NOFOLLOW` on every component, rejects multiply linked files, and avoids
  symlink traversal/path-swap races. The configured workspace is an
  administrator-selected trust boundary.
- `shell.run` remains opt-in: it requires `tools.shell_enabled: true` (the legacy
  `shell_sandbox_executable` field remains an opt-in alias; its value is not executed)
  and an independent workforce grant. Missing workforce policy remains `deny`.
  `tools.sandbox_enabled` is optional: when omitted, sandboxing is required on supported
  Linux x86_64/aarch64 and disabled by default on macOS. An explicit `false` permits an
  unconfined native child; an explicit `true` requires the sandbox backend and is
  rejected on unsupported platforms. For MCP stdio, `sandbox.enabled` is an optional
  per-server override that takes precedence over the shared setting. Omission is
  distinct from `false`; older configs that omitted the MCP flag now use the
  platform-dependent default.

  Required Linux confinement uses CCM's pre-Tokio self-exec Landlock/seccomp helper;
  it does not invoke or require Bubblewrap. Landlock ABI 6+, `openat2`, `close_range`,
  and successful setup are required. ABI/setup failure rejects execution and never
  silently falls back; explicitly setting sandboxing to `false` is the opt-out. The
  helper grants read/execute access to canonical system runtime trees (broader than
  executable files alone; `/usr/share/doc` was listable in the accepted probe), and
  write access only to the configured workspace and private per-call scratch directory.
  MCP stdio gets only private scratch writable. Socket syscalls are denied, as are
  namespace/mount changes, alternate process groups/sessions, cross-process inspection,
  `io_uring`, and selected metadata-changing syscalls. `clone3` is denied with `ENOSYS`;
  legacy `clone` rejects namespace flags and `CLONE_PARENT` (`EPERM`).

  In explicit unconfined mode (and macOS's platform default), CCM still clears and
  rebuilds the child environment, closes inherited descriptors above stdin/stdout/stderr,
  bounds wall time and output, and attempts Unix process-group cleanup. A child can
  escape that process group, so cleanup is best effort. These controls do not provide
  filesystem/syscall/network isolation or Linux rlimits; the child retains the CCM
  user's ordinary OS access. On macOS, shell still requires its separate opt-in and
  workforce grant. Windows and other unsupported targets do not receive an implicit
  native-execution default; unsupported native configurations fail validation.

  The safe `config.get` projection includes effective shell/MCP stdio mode, backend,
  and reason. It includes no child command, environment value, or filesystem path and
  does not claim that confinement is runtime-ready. Linux reports sandboxing as
  required before runtime capability is known; each launch checks kernel setup and
  fails closed if it cannot establish confinement.

  In required Linux sandbox mode, cleanup is a process-group-kill guarantee, not a
  synchronous wait for every descendant. On awaited normal exit and timeout/error
  cleanup, CCM sends `SIGKILL` to the group and explicitly waits for/reaps the direct
  sandbox leader. On caller cancellation/drop, the drop guard sends `SIGKILL` to the
  group but cannot await reaping. CCM is not a child subreaper and does not `waitpid`
  each descendant: ordinary forked descendants orphaned when their leader exits are
  left to the host's init or configured subreaper for final reaping. In the accepted
  runtime probe, PID 1 was Supervisor and reaped transient orphan zombies after about
  one second. This is host behavior, not a CCM guarantee: a descendant can remain a
  zombie until the system reaper acts. `CLONE_PARENT` is denied so a sandbox target
  cannot create an untracked direct child of CCM. Unconfined Unix mode also attempts
  process-group cleanup, but a child can escape the group, so it is best effort only.

  These Linux confinement limits apply only when sandboxing is required. This is not
  VM or namespace isolation. `stat`-like metadata queries outside allowed
  roots may still reveal file metadata even when opening file contents is denied.
  Existing hard links inside a writable root are part of the administrator's trust
  boundary: a link there can modify the same inode through an outside alias. CCM
  rejects mount points detected beneath writable roots at launch, but this mountinfo
  check is not a defense against a privileged concurrent mount change. Direct
  cross-directory rename/`REFER` is denied; userland tools can still emulate a move
  with copy-and-delete inside the writable workspace. CPU, address-space, file-size,
  and open-descriptor limits are per process/file, not aggregate quotas. The legacy
  `max_processes` setting is ignored; there are no hard aggregate CPU, memory, PID, or
  disk quotas.

  Native platform and kernel availability must be checked on the deployment host;
  a configuration projection is not a sandbox runtime acceptance test.
- `http.request` accepts only GET/POST/PUT/DELETE, exact host/port entries in
  `http_allowed_hosts`, and bounded headers/body/results.
  Entries without a scheme imply HTTPS; non-default ports and plain HTTP must include
  the explicit scheme/port (for example, `http://127.0.0.1:8080`). Redirects are
  disabled. CCM does not pin DNS resolution; administrators must review allowed
  hosts and the runtime's network environment.
- All tool calls have bounded JSON input/results and request timeouts. Tool schemas
  use a restricted JSON Schema subset, bounded nesting, and reject remote references
  and undeclared object properties.

## WASM plugin format

Place one `.json` manifest and its referenced `.wasm` file beneath `plugin_dir`:

```json
{
  "name": "text_summary",
  "description": "Summarize provided text",
  "wasm": "text_summary.wasm",
  "input_schema": {
    "type": "object",
    "properties": {"text": {"type": "string", "maxLength": 12000}},
    "required": ["text"],
    "additionalProperties": false
  },
  "capabilities": []
}
```

The embedded interpreter uses Wasmi. A module must export `memory`,
`ccm_alloc(i32) -> i32`, and `ccm_run(i32, i32) -> i64`. CCM passes the input
arguments as JSON bytes to `ccm_run`; the returned `i64` packs output pointer in
the high 32 bits and output byte length in the low 32 bits. The output must be JSON.
Modules may not import host functions or WASI. They are bounded by module size,
linear-memory size, instruction fuel, input/output size, and schema limits. A plugin
may return a pure JSON result or request one declared embedded capability using
`{"tool":"file.read","arguments":{...}}`. That capability is separately
authorized by the same permission broker; nested plugin/MCP dispatch is not allowed.
The plugin directory is scanned at model-call time; installed plugins do not need a
binary rebuild.

## MCP clients

Each `tools.mcp_servers` entry requires an administrator-reviewed profile with
`reviewed: true`, a non-unreviewed `reach` (`local`, `private_network`,
`public_network`, or `unrestricted`), and a non-unreviewed `impact` (`low`, `medium`,
or `high`). The profile records administrator judgment; it is not cryptographically
verified. Exact tools and their bounded input schemas are configured locally. CCM
does not issue `tools/list` to providers for discovery, so there is no provider
contact just to build model context and no provider response can widen its tool
allowlist.

Example Streamable HTTP provider:

```json
{
  "docs": {
    "profile": {"reviewed": true, "reach": "public_network", "impact": "low"},
    "transport": {"transport": "streamable_http", "url": "https://mcp.example.test/mcp"},
    "tools": {
      "search": {
        "description": "Search approved documentation",
        "input_schema": {
          "type": "object",
          "properties": {"query": {"type": "string", "maxLength": 2048}},
          "required": ["query"],
          "additionalProperties": false
        }
      }
    }
  }
}
```

For stdio, use `"transport":"stdio"`, an absolute `command`, static `args`, and an
optional `env` mapping from child environment names to names of CCM process
environment variables. Omit `sandbox.enabled` to inherit `tools.sandbox_enabled` or the
platform default; set it to `false` to explicitly run this provider unconfined, or
`true` to require sandboxing. In required Linux mode, providers get only private scratch
writable, not the configured workspace, and socket syscalls are denied. In unconfined
mode those filesystem and network restrictions do not apply. Environment is cleared
except for the fixed runtime variables and explicit mappings in either mode.
Streamable HTTP redirects are disabled; bearer tokens can be referenced with
`bearer_token_env` and are not stored in config.

MCP provider and tool grants use the same workforce/agent/stage policy described
above. A server existing in config and having a reviewed profile does not authorize
its tools. Configured schemas, total model-facing schema bytes, number of tools,
provider response bytes, and call duration are bounded. The implementation sends
initialize, initialized, and tools/call messages per tool invocation; it does not
maintain a long-lived MCP session or cache provider schemas. For stdio and Streamable
HTTP, incoming notifications and responses with unrelated JSON-RPC IDs do not satisfy
the pending request. Responses are matched by ID; the Streamable HTTP SSE parser
accepts incremental LF, CRLF and CR line endings and multiline event data within the
configured bounds. The tool/MCP review and final integrated verification are complete.

## Limits and integration notes

Defaults: 16 KiB tool input and result, 8 KiB per schema, 32 KiB total schema context,
64 tools, 2 MiB WASM module, 16 MiB WASM memory, 1,000,000 WASM fuel, and 10 seconds
per tool operation. When Linux sandboxing is required, defaults are 10 CPU seconds,
512 MiB address space, 64 MiB per file, and 64 open descriptors; set optional
`tools.sandbox_limits` values within their bounds. These resource limits do not apply
to unconfined native children. The legacy `max_processes` value is retained for config
compatibility but is not applied because `RLIMIT_NPROC` is UID-wide, not a per-job
limit. These limits do not provide aggregate resource quotas. Oversized results are
rejected, not silently truncated.

Client-declared tool calls continue to pass through unchanged. If a model emits CCM
and client tools in the same batch, CCM performs no operation from that batch and
returns a request to separate them. If internal tools ran in earlier rounds before a
client tool is requested, their observations are included in the returned assistant
message so a client continuation retains that context.

The process-wide validated `tools` snapshot is installed by `Config::from_env` at
startup and read by the nested `runtime::tools` module. This avoids changing existing
`AppState` construction sites; runtime policy still derives workforce/invoker/agent/
stage from authenticated state and the active execution payload, never from model
arguments.

### Foreman invocation subset — implemented in source

Foreman may optionally narrow the provider-facing CCM tools for an existing-agent
or fallback invocation, each async fan-out task, or each workflow stage. Omission
retains all policy-eligible `allow`/`ask` tools; an explicit empty subset exposes no
CCM tools. A subset cannot add a denied or otherwise ineligible tool. The selection
is retained with persisted continuations, while internal subset metadata is stripped
from client-visible protocol payloads.

New-format persisted continuation records carry a version marker and bind subset
lookup to the exact call batch. Lookup has no fixed 100-record cap, and a missing
versioned record fails closed rather than widening the tool set. Legacy continuations
without the marker use the policy-eligible default set.

For a creation route, the executor is unknown before generation, so the generator uses
the policy-eligible default set. CCM does not add a second post-creation routing
pass. Verification commands and platform limits are recorded in
[TEST_RESULTS.md](TEST_RESULTS.md).
