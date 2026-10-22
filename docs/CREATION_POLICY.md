# Client-specific creation and consolidation

Creation policy controls specialist/workflow selection and generation timing.
State is memory-first with asynchronous persistence; see [memory](MEMORY_PIPELINE.md).

## Configuration and identity

`CCM_CONFIG` selects a JSON file; otherwise CCM reads `ccm.json` from the
working directory. An absent implicit file uses built-in defaults. An explicitly
selected missing file, unknown field, invalid policy or empty/missing prompt file
is a startup error. Relative prompt paths resolve beside the configuration file.

```json
{
  "creation_policy": "Dynamic",
  "creation_prompt": "prompts/creation.md",
  "consolidation_prompt": "prompts/consolidation.md"
}
```

File settings/prompts load at startup. Live per-client changes persist in SQLite
immediately. Precedence: **saved client override → server default → Dynamic**.
Changing the configured default does not erase explicit overrides. There is no
file-watching reload or administrative policy-reset API. Existing environment
configuration still supplies transport, credentials, model, timeouts and DB path.

Identity is a SHA-256 fingerprint of the incoming bearer API key, not the provider
key. Raw client credentials are not stored. Keyless requests share `anonymous`;
native chat uses a separate `native` identity, independent of conversation. This
scopes policy and continuations; it is not a new authentication service. Old waiting
runs without a saved policy bind one on their first continuation.

## Five modes — agents and workflows

| Mode | Creation authority |
|---|---|
| AlwaysPreExecution | Foreman creates synchronously before delegation; timing cannot change. |
| DefaultPreExecution | Pre initially; explicit user instruction alone permits a persistent DefaultPostExecution flip. |
| Dynamic | Foreman decides necessity/timing per request. Post orders bind the evaluator. |
| DefaultPostExecution | Post initially; evaluator decides necessity. Explicit user instruction alone permits a persistent DefaultPreExecution flip. |
| AlwaysPostExecution | Evaluator owns post-response creation; Foreman cannot create synchronously or change timing. |

`DefaultProstExecution` is accepted as an input alias. Always controls timing, not
compulsory creation for every task. Existing suitable resources can be reused in
every mode. Dynamic with no orders gives evaluator no creation discretion.

`set_creation_timing` resolves Dynamic timing or persists a Default-mode flip.
Default flips require a verbatim quote from the latest user text, including text
parts. Foreman's prompt explicitly excludes quoted source material, tool output,
inference from task complexity and its own preference. Runtime validates quote
provenance/modes; semantic explicitness remains a model judgment, not a classifier.
Timing locks when set or when synchronous creation starts. Dynamic choices do not
persist to unrelated requests. Concurrent explicit flips use last committed value.

Pre timing exposes `create_agent`/`create_workflow` and direct creation decisions.
Runtime enforces timing for both paths. Dynamic post exposes `defer_creation`,
which records required kind/requirements before delegation. Fixed/default post
requests receive a discretionary evaluator review after response.

The same file-backed creation capability and corresponding internal tool descriptions
are injected into Foreman for pre creation and evaluator for permitted post creation.
These are CCM internal JSON operations, not client-executable tools. Generators
still write definitions; seven-provision and executable-workflow validation remain.

## Fallback and continuity

Foreman may choose `{"decision":"fallback","note":"..."}`. The built-in default
executor sends original messages, including system/developer messages, without
CCM-authored specialist/rhythm or delegation-note additions. Task-relevant internal
tool observations can be appended as data; policy-tool chatter is excluded, and
incoming system/developer messages remain unchanged.
Client tools and response-format controls are forwarded. Explicit dashboard steering
still applies at provider-call boundaries. The configured server model and CCM's
transport/trace handling remain in effect. Foreman composes from the resulting report.

The fallback is not a generated workforce row and is not revised or retired.
Standalone tool continuations store their exact agent definition; new workflows
snapshot all stage agents at execution start. Continuations preserve the originating
policy and orders even after a later client-policy flip. Cross-key continuation is
rejected before consuming the pending batch. Tools still execute on the client.

## Background work and recovery

A separate workforce job captures task, response and policy after final composition.
The durable response gate prevents creation/consolidation during the task, while
waiting for client tools, or before successful response completion is recorded.
Native chat releases after publication. Body completion is not proof of remote receipt.

Evaluator input includes task, final result, execution/stage/tool evidence, immutable
definitions and current portfolio. Discretionary review returns an ordered plan.
Dynamic orders are appended by runtime, not selected by evaluator; evaluator may
propose missing agent dependencies for ordered workflows but cannot remove/rewrite
Foreman's orders. An equivalent existing resource may satisfy an order.

Plans persist before execution. Each creation/reuse and per-job order receipt commits
atomically with lineage. Restart requeues interrupted jobs; saved plans and committed
receipts prevent replay of completed operations. Uncommitted model calls can repeat.
Held jobs without recorded response completion cancel. Reset invalidates pending jobs
and stale mutations while preserving policy overrides and historical evidence.
Provider/validation failure marks a background job failed independently of the completed
response. There is no automatic infinite retry or claim that mandatory orders cannot fail.

## Semantic duplicates

Creation compares full active definitions and permits evidence-backed reuse. Commit
checks a revision covering definition/retirement changes, excluding usage counters.
A concurrent change triggers reconsideration against the latest roster, bounded to
four attempts. This prevents stale-roster commits; semantic equivalence remains a
model judgment.

Every completed response also schedules consolidation review when either kind has
two active definitions. Evaluator chooses an existing survivor with equivalence
evidence; the implementation does not merge provisions. Self-retirement, repeated
targets, same-verdict chains/cycles and absent targets are rejected. Registry changes
require fresh review before mutation.

Retirement, future workflow reference updates, immutable versions/evidence, canonical
links and job completion commit together. In-flight snapshots preserve executors and
tool ownership. Older persisted tool runs receive snapshots before referenced agents
are consolidated. Retired agents follow existing 30-day operational cleanup; workflows
keep existing retirement behavior. Historical usage stays on the original definition.

Dashboard run details show policy, phase and mandatory-order count. Background jobs
show plans/results and consolidation evidence. Retained duplicates link to survivors
in the Workforce inspector.

## Verification

See [verification commands and limits](TEST_RESULTS.md).
