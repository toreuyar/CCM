# Foreman personality transparency

## Modes

- Server-side `personality_transparency` enum: `introvert`, `extrovert`, or `flexible`.
- Default: `introvert`. Set `flexible` to permit client preference changes.
- Flexible starts each client in introvert and permits explicit conversational
  switches, persisted per client/API key + selected exposed model and effective
  from the next request.
- Extrovert narrates live, before operations, in actual ordinary chat content:
  routing decisions, specialist/workflow reuse or creation, and impending work.
- Foreman provider reasoning and internal tool calls/results are separately
  exposed only in thinking blocks, not copied into ordinary chat narration.
- No operational narration is inserted into the final answer or after it.
- Formatted/output-only instructions constrain **chat content only**. They suppress
  narration, but do not suppress extrovert thinking.

These decisions supersede unconditional dashboard-only progress for extrovert
requests. Dashboard trace evidence remains available in both modes.

## Configuration

```json
{
  "personality_transparency": "flexible",
  "creation_policy": "Dynamic",
  "creation_prompt": "prompts/creation.md",
  "consolidation_prompt": "prompts/consolidation.md"
}
```

Loaded once at startup from `CCM_CONFIG` or `ccm.json`, with existing missing-
file/default behavior. An omitted personality field defaults to introvert; unknown
values fail startup. There is no live config-file reload. Fixed introvert/extrovert
ignore saved preferences and reject conversational personality changes.

In flexible mode, Foreman can call `set_personality` with `mode` (introvert or
extrovert) and a verbatim `user_instruction` from the latest user message. Intent is
interpreted by Foreman, not keyword matching; the runtime validates mode, configured
authority and quote provenance. Saved preferences live in `client_personality`.
API keys use the existing hashed identity, scoped to the exposed model; keyless
requests share `anonymous` per model and dashboard chats share `native` on the
default model. A request captures its effective mode before model
work; later writes cannot change that response. Both narration and exposed Foreman
thinking follow the selected mode. Response traces include configured and effective
personality, and changes emit `personality.preference_changed` receipts.

Preferences use the existing asynchronous database snapshot persistence; an abrupt
crash can lose an unpublished recent change. Fixed modes preserve saved preferences
for a later return to flexible. Example: “Switch to extrovert for this client.”

## Wire and presentation contract

| Mode/channel | Behavior |
|---|---|
| Introvert | Client sees final answer or real client tool calls. Foreman/internal reasoning stays in the dashboard inspector. |
| Extrovert chat | Pre-action paragraphs in SSE `choices[0].delta.content`, when the output contract permits them. |
| Extrovert thinking | Provider-returned Foreman reasoning plus labelled decision/internal-tool observations in `delta.reasoning_content`. No `<think>` tags in chat content. |
| Final answer | Provider content follows pre-action narration; no new operational narration is appended. Composition thinking is forwarded only before the first answer-content/refusal chunk. |
| Non-streaming API | `message.content` is answer-only: a buffered API cannot provide live narration. Extrovert thinking is returned separately in `message.reasoning_content`. |
| Native chat | Live narration/thinking uses the durable event stream; saved messages keep pre-answer narration in `ccm_narration`, separate from final `content`. |

Actual client tool calls still use normal OpenAI `tool_calls` and retain their
continuation ownership rules. Internal calls are textual observations inside
thinking; they are never emitted as executable client tool calls. Specialist,
generator and evaluator raw reasoning is not added to the client's thinking
channel. Provider reasoning fields are normalized to `reasoning_content`; no
reasoning is fabricated when the provider supplies none. Operational observations
are explicitly labelled as such.

OpenCode V2 provider documentation confirms the OpenAI-compatible provider path.
Read-only inspection of the local OpenCode `packages/llm/src/protocols/openai-chat.ts`
confirms `delta.reasoning_content` is parsed as reasoning, independently of content;
its lifecycle helper supports reopening thinking after a narration paragraph.
This is source evidence, **not** live OpenChamber rendering acceptance. Clients
that ignore this extension will omit thinking rather than receive it in content.

## Output restrictions

No chat narration is sent before the output contract is resolved. Explicit
non-text `response_format` forces narration off, independent of model output.
For natural-language constraints, the normal routing decision must include
`chat_narration_allowed: true|false`, evaluated against the complete original
role-labelled messages and response format. Missing/invalid/uncertain permission
means quiet chat. Later decisions may disable narration, never reopen it after a
suppression decision. Thinking permission remains independent.

There is no keyword taxonomy for user intent. Recognizing restrictions written in
prose is a model judgment; scripted tests do not establish live semantic accuracy.
The hard format gate and default-to-quiet behavior are runtime enforcement.

Client-tool continuations bypass routing, so extrovert continuations perform a
separate Foreman output-contract check before resuming. It cannot choose a new
executor or execute operations. Explicit structured response formats can skip
this check because narration is already prohibited. Introvert continuations keep
the existing no-Foreman-call path. A failed contract call fails the continuation
request under the existing provider/continuation recovery limits.

## Ordering, isolation and retries

Routing attempts now remain buffered until the decision is locally validated.
Only valid-attempt reasoning is released; failed attempts remain dashboard evidence.
This supersedes live forwarding of unvalidated routing thoughts, while preserving
separate thinking and pre-operation narration. See [FOREMAN_ROUTING.md](FOREMAN_ROUTING.md).

Runtime announcements use validated routing/resource information and concise
decision notes, before the corresponding model/tool/workflow action. Initial
review has no chat announcement until permission is known. Selection/creation
announcements are followed by task/stage progress, then a final pre-composition
announcement. Narration is closed before composition or real client tool handoff.
The final composition prompt explicitly prohibits further operational narration.

Presentation is task-local and bound to a request ID, not a global sender map.
Spawned post-response improvement jobs do not inherit the channel. Finishing the
response closes presentation. Native history excludes `ccm_narration` from
subsequent model inputs. The configured default executor retains its unchanged
client-instruction contract.

A model attempt that has exposed thinking live cannot automatically retry, just
as an attempt that has exposed content cannot. Earlier operation announcements
do not prevent retries of a later, as-yet-unexposed call. Buffered non-streaming
failed-attempt thinking is discarded before retry. Interrupted attempts retain
dashboard evidence; operational restart notices obey the narration gate and
final-answer cutoff. See [UPSTREAM_RELIABILITY.md](UPSTREAM_RELIABILITY.md).

Durable `response.presentation`, `response.contract`, `response.delta`,
`response.reasoning.reset` and `response.presentation.finished` events retain
presentation evidence. Run-detail snapshots reconstruct each request separately,
independent of the bounded timeline page. Frontend replay uses the snapshot cursor
to avoid duplicate narration/thinking after reconnect.

## Verification

See [verification commands and limits](TEST_RESULTS.md).
