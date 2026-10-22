# Execution and recovery contracts

CCM exposes an OpenAI-compatible chat endpoint, an authenticated web API and
Streamable HTTP MCP. See [installation](INSTALLATION.md),
[configuration](CONFIGURATION.md) and [authentication](BEARER_AUTH.md).

## Request flow

1. Authenticate the client key and check route/workforce permissions.
2. Resolve the public model alias, operational settings and client preferences.
3. Resume a valid returned tool-call batch, or ask Foreman to route the task.
4. Select/reuse a specialist or workflow, generate a definition when authorized,
   or use the prompt-transparent fallback. Foreman can also delegate agent fan-out.
5. Execute the selected work under model-call, tool-round and workflow limits.
6. Compose the final response through Foreman while preserving client output
   instructions. Internal reasoning/narration exposure follows personality mode.
7. Release eligible background creation/improvement work at the final response
   publication boundary, not merely when inference finishes.

One task may make multiple provider calls. Neither orchestration nor an Evaluator
verdict establishes improved answer quality. Diagnostics and traces distinguish
provider failures, retries, model decisions, tool effects and unknown outcomes.

## Definitions and workflows

Generators author reusable agent/workflow definitions; deterministic validation
rejects malformed definitions. Agents have identity, mission, method, boundaries,
task-context discipline, capabilities and an output contract. A described
capability is not a permission grant.

Workflow graphs contain explicit stages, branches and bounded cycles. A workflow
has one active stage at a time; Foreman's parallel agent fan-out is a separate
mechanism. Continuations retain their executing definition snapshot, tool selection,
client ownership and resolved model settings. Revisions apply to future selection
instead of silently changing a paused execution.

Definitions, revisions, usage and retained retirement evidence can be inspected
through the [workforce interface](WORKFORCE.md). Creation timing and duplicate
consolidation follow [creation policy](CREATION_POLICY.md).

## Client tools and embedded tools

OpenAI-style client tools execute in the originating client. CCM returns tool
calls and resumes after receiving the correct result batch; another client cannot
take ownership. The native dashboard is not a general client-tool executor.

CCM's embedded tools, WASM plugins and configured MCP clients are a separate
permission-controlled path. Exact grants and current authority are checked before
execution. `ask` pauses for a one-time approval owned by the invoking key; it does
not create a durable allow grant. See [tools](TOOLS_AND_MCP.md).

Linux native-child confinement requires supported Landlock/seccomp. macOS has
no native sandbox backend; enabled children are unconfined by default. The
documented Windows route is Linux inside WSL2, with kernel capabilities verified
on the actual host. Do not equate a configured mode with runtime sandbox proof.

## Asynchronous operations

Explicit async work returns an operation handle with status, result and cancel
URLs. State is managed by a bounded process-wide supervisor. API reads are scoped
to the invoking key/workforce, not global observation of every owner's work.

Default per-operation profile:

| Limit | Default |
|---|---:|
| Active execution time | 120 minutes |
| Logical model calls | 48 |
| Child workers | 4 |
| Nested async depth | 1 |
| Workflow stages | 16 |
| Client-tool wait / approval lifetime | 24 hours |
| Foreman check-in interval | 5 minutes |

Provider-token accounting is best effort, not an admission gate. A model-call
timeout remains separate from the operation's active-time budget.

The administrative capacity interface uses these server defaults:

| Setting | Default | Validation |
|---|---:|---|
| `max_root_operations` | 4 | 1–256 |
| `max_child_workers` | 4 | 1–256 |
| `max_queued_roots` | 8 | 0–4096 |
| `max_queue_wait_seconds` | 900 | 1–86400 |

`GET`/`PUT /api/admin/async-limits` read/replace server limits;
workforce overrides are managed at `/api/admin/workforces/{id}/async-limits`.
Omitted workforce fields inherit server limits, and supplied fields must fit
within them. Changes affect new admissions, not cancellation of active work;
these capacity settings are in memory and are not saved into `ccm.json`.

Pending client-tool handoffs have a separate bounded allowance, so terminal-result
reclamation cannot silently evict a live continuation. Completed results may expire
or be reclaimed under memory pressure; `result_expired`/HTTP 410 is different from
a failed operation. Fetch results promptly and distinguish unavailable bodies from
unknown or interrupted execution outcomes.

## Publication and improvement

Response completion means the server's body-delivery boundary, not proof that
the remote application consumed it. Client-tool handoffs keep the final-publication
gate held. A retrievable async result can be fetched again if its first read is
abandoned; it does not trigger duplicate completed work.

Sparse conversation checkpoints use prompt screening before wider evidence
evaluation. Shared/multi-key changes require the documented review/release rules.
Eligible single-key private workforces can automatically apply changes when
permissions and current definition/epoch checks allow it. See
[improvement](AUTO_IMPROVEMENT.md) for exact authority and recovery limits.

## Memory-first persistence

SQL changes become visible in bounded memory-backed state before disk publication.
The persistence worker writes complete snapshots independently of execution.
A crash can lose recently acknowledged but unpublished changes. The memory budget
does not cap total process RSS. See [memory](MEMORY_PIPELINE.md).

Run one CCM writer per database. Back up configuration and database after a clean
shutdown; an arbitrary live file copy is not a consistent application-level backup.

On restart, committed creation/application receipts are reconciled rather than
replaying known-completed effects. Unproven interrupted execution is reported as
interrupted with unknown outcome. Pending approvals are not executed automatically;
valid client-tool handoffs and durable completed results can be restored. Never
interpret process restart as permission to replay uncertain external side effects.

## Graceful shutdown and planned drain

`SIGTERM`/`SIGINT` closes new-work admission, applies HTTP grace, drains accepted
async/background work, then publishes durable state and flushes the traffic journal.
Cleanup and worker joins are bounded. Failure to flush is an error, not a successful
shutdown claim. The default configured application budget totals 195 seconds,
within a 300-second external stop allowance.

For planned restarts, use authenticated `POST /api/admin/service/drain` and inspect
`GET /api/admin/service/status` before stopping. Drain is monotonic: it cannot be
undone in that process. Readiness is 503 while draining; liveness can remain 200.
If safe-stop conditions do not arrive within the chosen deadline, preserve the
process and investigate rather than force-killing it or starting a second writer.
See [deployment](DEPLOYMENT.md) for the procedure.

## Traffic and telemetry

`CCM_TRAFFIC_LOG` selects the full JSONL journal. It records client request bodies,
dispatched response frames and trace events. Provider requests and response data
also enter SQLite traces under collection limits. Authentication headers are masked,
but secrets inside messages/tool payloads can remain. No automatic journal rotation
or general payload redaction is implemented.

Operational logs and optional OTLP export supplement the local trace history.
Export is best effort, not the persistence authority. See
[observability](OBSERVABILITY.md) and [security](../SECURITY.md).
