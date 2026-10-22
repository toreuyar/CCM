# Live workforce catalogue and usage

The dashboard and authenticated API expose the generated workforce and its usage.
See [verification](TEST_RESULTS.md) for test commands and platform limits.

## Catalogue and definitions

The Workforce tab lists generated agents and workflows (active and retired), with
search, kind and status filters. Built-in meta-agents are excluded. Selecting an
entry shows the full persisted definition, including the agent's seven-provision
system prompt or workflow stages/branches/budgets. Related agents/workflows are
linked. Immutable versions, version comparison, change evidence and evaluations
are inspectable, including history for a removed agent referenced by a workflow.

`GET /api/workforce` returns entities, definitions, usage windows, last assignment
and model-call timestamps, `tracking_since_ms`, `as_of_ms` and a durable event cursor.
Database triggers emit global `workforce.changed` events for registry and usage
changes. The existing replayable SSE connection drives debounced snapshot refresh;
reconnect refreshes the snapshot. The SSE loop checks for durable writes at least
every second even without a process-local wakeup. Idle window counts refresh every
minute while Workforce is open, so usage ages out without another model request.

## Counting contract

- Direct agent assignment: once per owning execution/agent. Tool continuations
  share that execution and do not add an assignment.
- Workflow agent assignment: once per workflow-run/step/agent; a loop iteration is
  a new step. A tool continuation of that step does not add an assignment.
- Workflow assignment: once per workflow run, including resumed runs.
- Model call: every started specialist provider attempt, including interruptions,
  failed attempts and tool continuations. Foreman/generator/evaluator calls do not
  count as calls by the specialist they discuss. Workflow totals include its stage
  agents' calls. Internal workflow attribution is removed before provider dispatch.
- Attempts are recorded before provider work; assignments before work begins.
  Unique persisted keys prevent duplicate assignment counts across continuation.
- Windows are `(as_of - duration, as_of]`: previous 1h, 6h, 24h, 7d and rolling 30d.
- Detailed counting begins when this schema is first installed. Old cumulative
  counters cannot reconstruct these windows and are not backfilled. The UI states
  the tracking start explicitly. Usage records survive reset and operational purge.

Execution IDs and prompt construction remain separate; no new model calls are
introduced for catalogue reporting. Provider cache performance is not verified here.

## Retired-agent lifecycle

Retirement immediately excludes agents from selection and new evaluation calls.
Retired agents remain in the catalogue for 30 days from retirement, then their
operational registry rows are removed. A startup/60-second maintenance task performs
cleanup. Before deletion, definitions are archived in the existing immutable store;
versions, evaluations, trace evidence and usage records are not deleted.

Existing retired rows use the latest applied retirement evaluation timestamp when
available; otherwise retention starts at migration. A persisted retirement timestamp
survives process restart. Purging a retired agent does not rewrite workflow definitions
that reference it; their definition/history links remain inspectable. Existing workflow
execution rules still require an active stage agent except for a persisted tool-stage
continuation holding its own snapshot. Workflow retirement remains inspectable; this
30-day purge policy applies to agents as requested.

There is no automatic inactivity threshold or stale-agent deletion policy yet.
These usage reports provide the evidence for that later decision.
