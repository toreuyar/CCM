# Observability and web workspace

## Workspace

The dashboard is served from `/` (default `http://127.0.0.1:8080`). Assets
are embedded in the executable; there is no separate frontend service.
It includes native chat, external-run inspection, a workforce inventory and
background improvement panels. Hierarchy uses indentation rather than connectors.

- Sidebar: executions and native conversations, status/source filters, search,
  and older-run pagination.
- Main pane: hierarchical execution nodes, operation/token/timing summaries,
  ordered event timeline, native Foreman chat or external conversation inspection.
- Inspector: actual operation messages, provider-returned reasoning when present,
  tool calls/results, raw evidence, executed definition snapshot, line comparison
  between retained versions, and agent steering.
- Dark/light themes, resizable desktop panes, narrow-screen inspector overlay,
  live-follow toggle, and reduced-motion styling.

The display contains recorded decisions and provider output, not inferred private
reasoning. Missing token usage is displayed as unavailable. Native chat has no
client-tool executor; tools supplied by an external API client execute there.

## Correlation and history

`POST /v1/chat/completions` returns `x-ccm-request-id` and `x-ccm-run-id`.
External clients can supply `x-ccm-conversation-id`, or
`metadata.ccm_conversation_id`, to associate their requests with a conversation.
Without that identifier, CCM retains separate requests/runs rather than
guessing conversation identity from message similarity.

Tool-call batches have durable trace ownership. A complete batch of results
automatically resumes the original run; an optional `x-ccm-run-id` or
`metadata.ccm_run_id` must agree with that owner. Partial, duplicate, mixed-owner,
reset and already-consumed results are rejected. A trace owner is consumed before
resumed model work; crashes do not cause automatic replay.

Each model attempt records its input, result, parent span, definition version,
duration and available usage. Workflow stages retain the executed graph and
specialist snapshots. Generated definitions are archived before deletion;
evaluations retain before/after definitions and applied/conflict evidence.
Startup imports snapshots from existing portfolio/evaluation records without
pretending historical detailed traces existed.

`debug-delete` remains exact-match and `CCM_DEBUG` gated. It deletes generated
agents, workflows and paused workflow state. It retains operational events,
evaluations, traces, definition versions and native conversation history. Active
traces become `reset`, pending tools are invalidated, and queued guidance becomes
`not_delivered`. Built-in meta-agent prompts remain available.

## Live delivery and API

| Endpoint | Contract |
|---|---|
| `GET /api/runs?offset=0` | Recent 100 runs, next page offset, global event cursor |
| `GET /api/runs/{id}` | Spans, controls, versions, changes, recent non-token events, snapshot cursor |
| `GET /api/events?after=N&run=ID` | SSE `trace` events; optional run filter; `Last-Event-ID` overrides `after` |
| `GET /api/events/page?after=N&run=ID` | Up to 500 ordered events, strictly after N |
| `POST /api/runs/{id}/control` | `{target, action, message}`; target is an agent ID or `*` |
| `GET /api/definitions/{id}` | Retained versions, changes and evaluation history |
| `GET /api/runs/{id}/otlp` | OTLP/HTTP JSON representation of finished spans |
| `GET /api/statistics` | Existing grouped operational statistics |
| `GET /api/conversations` | Native conversations |
| `GET /api/conversations/{id}` | Native conversation messages |
| `POST /api/chat` | `{message, conversation_id?}`; returns conversation/run/request IDs |

SQLite sequence numbers are authoritative. Broadcast notifications wake SSE
readers; notification loss does not discard durable events. Reconnect starts
after the last received sequence. A detail snapshot includes accumulated partial
output plus its cursor; the browser applies only newer deltas.

Internal calls request provider streaming and aggregate the OpenAI message for
the existing orchestration machinery. JSON-only responses are also accepted.
Providers returning SSE must terminate with `[DONE]`; truncated streams fail.
Each provider attempt retains the configured HTTP/body timeout budget.

### Operation progress and personality

Operations emit `operation.progress` trace events for dashboard inspection.
Client-visible narration/thinking follows the configured personality and the
client's output contract; introvert keeps internal progress in traces. See
[personality](PERSONALITY_TRANSPARENCY.md) for extrovert/flexible behavior.

For `stream: true`, the response still opens before Foreman dispatch. The bounded
channel carries the permitted presentation output and client-tool calls.

Every streamed chunk uses the same completion ID, timestamp and model. Early
headers carry request/run correlation and the served model; the eventual selected
agent is recorded in trace evidence, since selection has not happened when the
headers are sent. `x-ccm-route` is `dispatch` for live streams, including those
ending with `tool_calls`; non-streaming tool replies retain route `tools`.
Tool chunks retain function names, arguments, ownership IDs and indexes.

Errors discovered after streaming starts are OpenAI-shaped error frames under
the already-sent HTTP 200 status, without a successful finish or `[DONE]`. Input
validation still occurs before streaming. Disconnects abort the whole pipeline,
including dispatch, and record cancellation. SSE comment heartbeats keep long
operations active; responses request no caching/transformation and disable
buffering for proxies that honor `X-Accel-Buffering: no`.

## Steering semantics

- `guidance`: queued text is appended as an operator-labelled user message before
  the next matching model call. Delivery records the receiving span ID.
- `interrupt`: the active matching HTTP/model future is dropped. A new attempt
  starts with guidance; the interrupted attempt and its partial deltas remain
  inspectable. If no matching call is active, guidance waits for the next call.
- `pause`: the matching execution stops at its next model-call boundary. The
  already-running call can finish; progression then waits.
- `resume`: clears that target's pause gate. A global pause must be resumed
  globally; clearing an agent gate does not clear the global gate.

Run-level `*` controls match any next model call. Agent controls use the actor's
ID, including `foreman`, `evaluator`, `agent-generator`, `workflow-generator` and
specialist IDs. Workflow containers and client/internal tool nodes are evidence
nodes; the inspector targets their model-backed agent operations for steering.
Controls are accepted/queued, delivered to a recorded attempt, or marked
not-delivered when the run terminates. Delivery is evidence of injection, not a
claim that the model obeyed it.

Cancellation never undoes client tools. Already-streamed composition text cannot
be retracted from an external client; an interrupt inserts an explicit restart
marker and subsequent content belongs to the new attempt. The web trace preserves
the separate attempts. Completed native messages use the replacement result.

## Recovery and export boundaries

After process loss, previously running/paused traces become interrupted; CCM
does not replay model or tool side effects. Waiting-for-tools runs retain their
ownership and stored workflow progress. Claimed workflow recovery remains subject
to the existing no-automatic-replay contract. Native background tasks survive a
browser disconnect, not a process restart.

Local evidence is durable independently of external telemetry export. Set
`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to opt into best-effort JSON span export.
Exports use service name `ccm`, W3C-shaped trace/span identifiers and definition
version attributes. Collector delivery has a five-second timeout and no durable
retry queue; the per-run OTLP endpoint permits explicit re-export. Propagated
`traceparent` identifies the actual model attempt.

Known storage boundary: model/definition evidence is locally persisted, but
lineage-event writes follow portfolio/evaluation commits. A storage failure or
crash in that interval can omit a change event; preserved evaluation snapshots
remain the recovery evidence. No distributed exactly-once guarantee is claimed.

Structured credential-named fields are redacted in trace artifacts; arbitrary
secrets embedded in natural-language message strings are not detected. The
workspace uses the existing CCM listener and introduces no authentication
layer. Model input/output retention follows the same local database lifecycle.

## Verification

See [verification commands and limits](TEST_RESULTS.md).
