# Conversation-wide improvement checkpoints

CCM schedules discretionary assessment at sparse conversation-wide checkpoints.
Private single-key workforces can apply eligible improvements automatically;
shared-workforce changes require the permission and release rules below.

## Recovery contracts

- **Reset epochs:** reset advances the durable epoch and cancels prior queued/running
  checkpoint work transactionally. Screening, release, and suggestion commits require
  the same current epoch, so old or late evidence cannot cross a reset boundary.
- **Legacy consolidation snapshots:** applying a shared agent consolidation preserves
  the retired executor snapshot in affected paused legacy runs throughout the
  workforce. This is an internal continuity update; it does not expose another key's
  private run state or evidence to the applying writer.
- **Suggestion receipts:** revise/retire suggestion rows and their job receipts commit
  atomically under stable idempotency keys. A requeued job recovers the existing
  suggestion rather than creating a duplicate or repeating its committed result.
- **Evidence coverage:** included/omitted counts and completeness are recomputed from
  the final bounded prompt, run, span, event, and registry arrays, including nested
  evidence, rather than relying on pre-truncation counts.

## Private-workforce autonomous application

Foreman can automatically apply create, revise, retire and consolidate improvement
decisions in an eligible single-key private workforce. Application authority is
separate from the checkpoint trigger policy.

- **Eligibility:** the source workforce must be private with exactly one active key,
  and that source key must currently have both `improvement_contribute` and
  `roster_write`. Privacy is determined from authoritative membership and permissions,
  not the workforce's name. Capture/enqueue records AutoPrivate intent only when this
  predicate holds; the final mutation transaction checks it again together with
  source/job ownership, intent, current reset epoch, and the applicable definition
  snapshot/revision. Consolidation enqueue uses the same predicate transactionally.
  If eligibility is lost, the suggestion remains pending; the private path cannot
  resolve a shared handle or fall back to shared application. Shared and private
  multi-key workforces do not receive automatic application; shared cross-key create
  proposals additionally require explicit source release and authorized application.
- **Creation ownership:** evaluator verdicts contain requirements only. The evaluator
  does not produce or register a definition. Foreman delegates creation to the
  dedicated registry generator/validator pipeline, which may reuse an equivalent
  definition or register a validated one.
- **Durable plans and recovery:** the source job stores an exact indexed creation plan
  before generation. A stable job/kind/index key binds each plan item and receipt to
  its source job and key. Registration, receipt, suggestion result and job outcome
  commit atomically after final authority, epoch, intent and registry checks. Provider
  inference may repeat if interrupted before commit; a committed receipt is replayed
  instead of creating a second registration. A multi-item job does not report
  completion while any required plan receipt or consolidation suggestion remains
  pending.
- **Shared create privacy:** shared create proposals remain private to their source
  until that key explicitly releases/declassifies them. Cross-key application requires
  current `roster_write` authority and the source release; cross-key discovery retains
  its `roster_read` plus `roster_write` requirement, and release alone does not apply.
  Cross-key responses use an opaque share handle and allowlisted resource result,
  omitting contributor identity, source suggestion/job/checkpoint/conversation/request
  IDs and internal job status. Source-owner views retain source-only details.
- **Trigger policy unchanged:** turn, elapsed-time and provider-reported foreground-
  token settings, prompt-only screening, coalescing, and the exclusion of explicit,
  task-required and policy-owned work from discretionary gating remain as documented
  below. No operational-metric trigger was added.

## Scheduling and evidence

Checkpoint state is durable and scoped by conversation, workforce and originating
API key. The settings are independently configurable in the server JSON config:

```json
{
  "improvement": {
    "turns": {
      "enabled": true,
      "initial_interval": 10,
      "minimum_interval": 5,
      "maximum_interval": 30
    },
    "elapsed": { "enabled": true, "interval_ms": 3600000 },
    "foreground_tokens": { "enabled": false, "threshold": null }
  }
}
```

Turn checkpoints count completed user-facing turns, not internal model/tool calls.
The first default checkpoint is 10 turns; the prompt-only screener selects the
following interval within the configured 5–30 bounds. Elapsed checkpoints default
to one hour. Token checkpoints count provider-reported foreground usage only and
remain disabled without a threshold. Missing or incomplete usage is unknown, never
zero. Overlapping due triggers coalesce into one queued checkpoint. An elapsed
worker schedules from its own clock, independently of a later foreground request or
response, when released user prompts remain unscreened. It does not repeatedly screen
unchanged input.

Each successful foreground response captures its latest user prompt and its
attributed run evidence. Capture remains held until response completion: ordinary
HTTP responses release after the body is delivered, native chat after the assistant
message is persisted, streamed async after its final body, and non-streaming async
after the completed `/api/operations/{id}/result` body is delivered. An abandoned
async result read leaves work held so the result can be fetched again. Failed or
incomplete execution is cancelled. A prompt-only screener receives only user prompts;
full evaluation runs only after a positive screen and may inspect the scoped
conversation traces, tools, versions and outcomes. A negative screen makes no
evaluator call. Unassessed discretionary jobs remain at `pending_checkpoint` for a
later positive checkpoint.

HTTP, MCP and async final-publication guards are run-scoped: they verify that the
current request belongs to the owner's trace run and release held response work only
after successful final-body delivery. A delivered client-tool handoff keeps the gate
held; abandoning a nonretryable response cancels still-held work for that exact run.
Dropping a still-retrievable async result keeps its work held for retry, until delivery
or operation expiry. An MCP final JSON-RPC wrapper over 1 MiB is replaced with a
bounded error before the final message is persisted or the gate is released; rejected
nonretryable work is cancelled.

The feedback screener is an internal generated/registered specialist, not a
user-facing roster definition. Its input is limited to user-authored prompt samples
and coverage metadata; it cannot inspect assistant output, tools, traces or
definitions. Captured prompts, sampled runs, spans, events and serialized evaluation
evidence are bounded. Coverage metadata records omissions or truncation, so a partial
sample is not treated as exhaustive evidence.

Explicit management, task-required checks and policy-owned post-response work are
not throttled by discretionary checkpoints. Policy-owned creation work remains on
its existing path. Automatic consolidation is a separate read-only review: it can
create a private pending suggestion, but it does not mutate the roster.

## Suggestions and permissions

Outside the eligible private-workforce path above, automatic evaluation and consolidation are review-only. They can create private pending
`revise`, `retire` or `consolidate` suggestions, but cannot mutate a shared definition
or create a new one. Consolidation review is limited to complete definitions and
bounded evidence; it must not recommend a duplicate from incomplete coverage. The raw
verdict, evidence, job/checkpoint/request identifiers, conversation and trace remain
private to the contributing key.

- `GET /api/improvements` requires `roster_read` or `improvement_contribute` and
  always includes the authenticated key's own suggestions. It includes other keys'
  pending suggestions only when the same key has both `roster_read` and
  `roster_write`; a read-only key sees no shared suggestions, and a writer without
  `roster_read` cannot list them. A contributor without `roster_read` retains only
  its own private view.
- A cross-key item is an allowlisted projection: an opaque share handle, action,
  entity kind/id, pending status, and the generic note “based on private checkpoint
  evidence; details withheld”. Retirement and consolidation actions may be shown as
  sanitized actions. Before source declassification, eligible shared writers receive
  metadata only, including that declassification is required; they do not see the
  proposed replacement. The originating key retains its full private proposal view.
  Declassification releases replacement visibility to eligible writers but does not
  apply the suggestion; application is a separate explicit action. Contributor
  identity, source conversation/traces, raw evaluator reason/evidence, and private
  job/checkpoint/request IDs remain excluded. `roster_read` plus `roster_write` is
  required to list cross-key items; the contributor retains its private view.
- The implemented HTTP action `POST /api/improvements/{id}/declassify` requires
  `improvement_contribute` and source ownership. It records the source key's action;
  it does not apply the suggestion or mutate a definition. MCP declassification and
  its source-scope, model-identity and heartbeat checks have been accepted in
  independent review, together with run-level final-publication guarding across the
  full request chain. Declassification releases revision visibility only; applying
  the suggestion remains a separate explicit action.
- `POST /api/improvements/{id}/apply` explicitly applies a pending suggestion and
  requires current `roster_write` permission. The handle is resolved only inside the
  caller's workforce, and the active definition must still match the captured
  snapshot; stale changes conflict. Application creates a writer-owned trace,
  evaluation and lineage record with the sanitized proposal and generic rationale,
  never linking to the contributor's conversation/trace or recording its private
  evidence. The contributor sees the suggestion's updated status, not the writer's
  private request or trace. For agent consolidation, legacy paused runs across the
  workforce retain the old executor snapshot internally; the applying key is not given
  another key's private run state or evidence. Mutation, lineage and status receipt
  commit atomically.
- `improvement_contribute` controls whether a key's private evidence may contribute
  to suggestions. It does not grant `roster_write`.

The dashboard currently displays per-run improvement job status but does not expose
the private suggestion list, declassification, or apply controls. Browser
bearer-key support is documented in [BEARER_AUTH.md](BEARER_AUTH.md); its isolated 20
assertions passed, but that is not broad manual UI acceptance.

## Queue, restart and recovery

Checkpoint counters, prompt evidence, checkpoints, jobs and suggestions are stored
durably. On restart, held work is cancelled because response completion cannot be
assumed; running checkpoints/jobs without a committed receipt become terminal
`interrupted` with an unknown outcome and are not requeued; queued policy work and
checkpoint-associated jobs remain queued; and an interrupted suggestion application
returns to pending. Legacy queued discretionary jobs without checkpoint evidence are
cancelled but retained as history. Startup recovery reconciles only durable receipts
and never calls a provider: a running job whose applied suggestions are all committed
becomes completed, a non-private pending receipt becomes `suggested`, and an eligible
private-auto job with a pending receipt returns to queued so the worker can apply that
receipt without repeating evaluation. A screener or read-only evaluation that was
running without a committed result is not retried automatically. Explicit application
is transactional, so a committed definition change cannot be applied a second time as
a new revision.

Bounded process shutdown is different from crash recovery. If the configured async
drain expires, the improvement worker is stopped and any claimed job/checkpoint still
in `running` is durably changed to `interrupted` with an unknown outcome, so startup
does not requeue and replay it. A job whose applied-suggestion receipt is already
committed is reconciled as completed from that receipt instead. Held jobs without a
recorded response release remain cancelled. The same startup rule covers an arbitrary
crash that leaves a durably running row behind. See [OPERATIONS.md](OPERATIONS.md) for
the shutdown budgets and fence.

The later durable-async update persists operation/approval records, results, handoffs,
and limit settings; active worker futures are not resumed after restart. This recovery
core received focused independent source acceptance: follow-up review closed the
restored-handoff expiry/capacity and exact-owner held-run-gate findings, with 14
handoff tests passed on the frozen source snapshot. Broader integrated verification
and deployment are not claimed; shutdown-budget/quiescence/join behavior and backend
handoff remain open. See [OPERATIONS.md](OPERATIONS.md) and [TEST_RESULTS.md](TEST_RESULTS.md)
for the accepted boundary. Earlier deployment history and live inference-smoke limits
remain as recorded above; a successful smoke does not establish general model quality.
No live database contents were inspected or modified as part of this documentation
update.

## Prior response-boundary contract

The inherited response gate remains in force: this is a server-side response
completion boundary, not proof that a remote client read the body. A durable
response-closure marker prevents late tasks from enqueueing work after cancellation.
Background evaluation does not delay the user's completed answer.
