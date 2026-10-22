# CCM MCP server

CCM exposes an authenticated Streamable HTTP MCP endpoint at `/mcp` (JSON-RPC
requests use `POST`; `GET` opens the SSE session stream and `DELETE` closes the
session).
The endpoint adapts existing CCM behavior; it does not create a second store,
executor, evaluator, or async supervisor.

## Connection and sessions

- Every request requires `Authorization: Bearer <key>` and external APIs must be
  enabled with the configured bootstrap credential.
- Send an MCP `initialize` JSON-RPC request with `protocolVersion`, `capabilities`
  and `clientInfo`. CCM currently negotiates `2025-11-25`, `2025-06-18` and
  `2025-03-26`.
- The initialize response returns `Mcp-Session-Id` and
  `MCP-Protocol-Version`. Send both headers, plus the bearer key, on subsequent
  POST/GET/DELETE requests. Sessions expire after eight idle hours and are bound
  to the authenticating key and workforce. A different key cannot take over a
  session. DELETE ends the session.
- POST accepts a single JSON-RPC message and returns JSON. Notifications receive
  HTTP 202 without a JSON-RPC body. GET opens an SSE connection with keepalives;
  CCM does not currently initiate server-to-client MCP messages. The endpoint
  accepts request bodies up to 2 MiB.
- MCP errors use JSON-RPC errors for protocol/method failures and `isError` tool
  results for tool permission, input, or CCM operation failures.
- The dashboard uses the same bearer-key model for its `/api` calls. Its access-key
  form and browser storage behavior are described in [Bearer authentication](BEARER_AUTH.md).

## Tools

The tools/list response is filtered by the authenticated key's permissions.
Tool calls check permissions again before executing.
Foreman's invocation-specific tool subset is separate from authenticated permission
filtering and is implemented in source. Its per-agent/fallback, fan-out-task,
workflow-stage, empty-subset, and continuation behavior is documented in
[Embedded tools and configured MCP clients](TOOLS_AND_MCP.md). Its versioned
continuation lookup and legacy fallback are part of that contract.

| Tool group | Behavior and permission |
|---|---|
| `foreman.chat`, `foreman.start_async` | Normal Foreman conversation or explicit async admission; `roster_use`. Conversations are private to the key. `foreman.chat` accepts exact client-tool result batches to continue a client-tool handoff. If an embedded CCM tool needs approval, `foreman.chat` returns a structured `waiting_approval` result and handles, not a fabricated assistant answer. |
| `foreman.conversations`, `foreman.conversation` | List saved conversations and read bounded message pages owned by the authenticated key; `roster_read`. |
| `workforce.list`, `workforce.get` | Scoped active roster; retired definitions are opt-in; `roster_read`. Listing returns compact metadata and bounded pages. |
| `workforce.create_agent`, `workforce.create_workflow` | Dedicated agent/workflow meta-agent creation and existing registry reuse/validation; `roster_write`. |
| `workforce.evaluate` | Runs the real Evaluator against an active definition and supplied task/result evidence. The Evaluator may keep, revise, or retire; `roster_write`. Retirement is not a direct delete: CCM removes the definition from selection while retaining retirement/history evidence. |
| `workforce.run_agent`, `workforce.run_workflow` | Executes the explicitly selected active definition through CCM's runtime, then uses Foreman composition; `roster_use`. If an embedded CCM tool needs approval, returns `waiting_approval` while that exact run is paused. Optional client tools continue through `foreman.chat`. |
| `operations.status`, `operations.result` | Read the existing async supervisor; `roster_read`. Operation ownership is checked against both key and workforce. |
| `operations.cancel` | Cancel an owned supervisor operation; `roster_write`. |
| `operations.continue` | Resume an async tool handoff via the existing chat-completions continuation path; `roster_use`. The supplied messages must include the complete pending tool batch. |
| `tool_approvals.status`, `tool_approvals.approve`, `tool_approvals.deny`, `tool_approvals.cancel`, `tool_approvals.result` | Inspect and resolve only the current invoker's one-time approval; all require `roster_use`, not `roster_write`. Result polling returns the resumed synchronous result; async approval results are not cloned and remain on their original operation handle. |
| `observability.runs`, `observability.run`, `observability.events`, `observability.statistics`, `observability.improvements` | Read the existing trace/statistics/improvement data; `roster_read`. Runs/events/details have bounded pagination and filters. |
| `improvements.suggestions` | Existing private suggestions and policy-approved shared projections; `roster_read` or `improvement_contribute`. Shared projections retain CCM's existing `roster_read` + `roster_write` rule. Generated revision text remains private until the source key explicitly declassifies it. |
| `improvements.declassify` | Source-key scope/ownership, model-identity, heartbeat, and run-level final-publication checks are accepted in independent review. The action requires `improvement_contribute`, records declassification only, and does not apply or mutate the suggestion. |
| `improvements.apply` | Apply an existing conflict-checked suggestion; `roster_write`. |
| `config.get` | Safe read-only projection for the authenticated key; `roster_read`. Ordinary keys receive only key/workforce-scoped settings, exposed model identity, sandbox status and their own workforce async limits; server-wide configuration, provider timeouts, memory budget, improvement configuration and server async limits are withheld (`bootstrap_admin` only). It excludes credentials and environment secret values. |
| `config.async_limits.get` | Read the live async supervisor's server limits, all workforce overrides, active counts and queue depth; `bootstrap_admin`. |
| `config.async_limits.set_server` | Replace all four live server limits through the existing supervisor operation; `bootstrap_admin`. Bounds: root operations and child workers 1-256, queued roots 0-4096, queue wait 1-86400 seconds. All fields are required. |
| `config.async_limits.set_workforce` | Set bounded overrides for an existing workforce through the existing supervisor operation; `bootstrap_admin`. Each limit is optional; omitting a field clears that override. Values must also fit within current server limits. |
| `admin.workforces.*`, `admin.keys.*` | Existing workforce/key administration only; `bootstrap_admin`. Key lists never include bearer tokens. Key creation returns its new token once, matching the existing administration API. |

The only mutable configuration subset exposed by MCP is the existing live async
supervisor server limits and per-workforce overrides. These changes take effect
immediately in the running process and are persisted in the async state store for
recovery across restart; they are not written to `ccm.json`. Active worker futures
are not resumed after restart. Independent follow-up review closed the restored-handoff
expiry/capacity and exact-owner held-run-gate findings on a frozen source snapshot;
14 focused handoff tests passed. This does not establish full integrated verification
or deployment. Shutdown-budget/quiescence/join behavior and backend handoff remain
separate open integration work. See [OPERATIONS.md](OPERATIONS.md) and
[TEST_RESULTS.md](TEST_RESULTS.md) for the accepted source boundary.
Inputs are strict objects with no additional properties; `workforce_id` is
limited to 1-128 characters. Workforce fields use the same absolute numeric
bounds as the server fields above and are additionally checked against current
server limits. Other effective CCM settings remain read-only through
`config.get`.

`tool_approvals.result` is an existing-work operation: it may complete the original
run's publication after the body is delivered, so it acquires the shared
existing-work admission lease and remains counted until both delivery and that
publication finish. Plain status/result reads for async operations do not acquire a
new-work lease. During shutdown, retrieval is accepted while existing-work admission
is open and rejected once the lifecycle is sealed.

The configured bootstrap key receives only bootstrap-admin access by default; it
does not implicitly receive roster, run, or observability permissions. Admin
tools are not exposed to ordinary workforce keys.

### One-time tool approvals

Approval handles are issued only after CCM pauses before an embedded tool's side
effect. MCP returns `status: "waiting_approval"`, the exact operation and one or
more handles; this is not an assistant completion. Use `tool_approvals.status` to
inspect a handle, then call exactly one of `tool_approvals.approve` or
`tool_approvals.deny`. A vote is one-time and bound server-side to the bearer key,
workforce, request/run, tool definition, agent/stage and arguments. A peer key in
the same workforce cannot inspect or resolve it. Approval resumes the same paused
model/workflow; denial resumes it with a blocked-operation observation. Neither
decision updates tool policy or writes an `allow` grant. No tool side effect runs
before approval.

Use `tool_approvals.result` to poll the original synchronous run for its eventual
result. If the handle has an `operation_id`, continue polling that same operation
with `operations.status`/`operations.result`. `tool_approvals.cancel` cancels the
invoker's pending approval and paused run; because it cannot mutate another
invoker's work, it requires `roster_use` only. MCP calls require the matching
session and the current bearer identity on every request. MCP request bodies are
limited to 2 MiB, approval IDs to 128 characters, and serialized tool results to
1 MiB.

Final MCP publication is gated on delivery of the bounded public JSON-RPC body. A
serialized final wrapper over 1 MiB is replaced with a bounded tool error before the
final message is persisted or run-level improvement work is released; nonretryable
held work is cancelled for that exact owner-scoped trace run. Delivered client-tool
handoffs persist their public message but keep the run's held work. Async results
remain on the original operation handle, and dropping a still-retrievable result keeps
its work held for retry or operation expiry. These publication guards were accepted in
independent review.

## Data scope, redaction, and pagination

Roster and usage reads use the authenticated workforce scope. Conversations,
runs, events, trace details, improvement jobs, suggestions, and supervisor
operations retain their existing per-key ownership checks; shared workforce
visibility does not make another key's private activity visible. Trace and
configuration outputs recursively redact secret-like fields. The one-time
credential returned specifically by `admin.keys.create` is the sole intentional
secret-bearing response; it is not included in key lists or observation output.

Pages default to 50 items and are limited to 100. Roster entries, conversations,
run lists/details, statistics groups, and improvement suggestions are paginated.
Run lists support status/source filters; events support a sequence cursor and
run/kind filters; run details support span/event kind filters; statistics groups
support kind/entity/status filters. Use `next_offset` or `next_after` to continue.
Large outputs are not returned as unbounded arrays by the MCP surface.

## Deliberate limits

Only the `tools` capability is advertised. CCM does not advertise MCP prompts,
resources, generic configuration writes, direct `ccm.json` edits, raw SQL, or
unconditional definition deletion. The async-limit operations above are the
only configuration mutations and use the existing bootstrap-admin validation
and live supervisor paths. Definition changes use the existing Evaluator
authority and revision-checked mutation path; background suggestions remain
unapplied unless the existing suggestion-application operation accepts them.
