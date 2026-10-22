# Provider-call reliability

Verification: [TEST_RESULTS.md](TEST_RESULTS.md). Provider reads use memory-first tracing
and buffered delivery: [MEMORY_PIPELINE.md](MEMORY_PIPELINE.md).

## Startup configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| `CCM_UPSTREAM_CONNECT_TIMEOUT_MS` | 10000 | Connection establishment |
| `CCM_UPSTREAM_FIRST_RESPONSE_TIMEOUT_MS` | 60000 | Response headers, then separately first nonempty body bytes |
| `CCM_UPSTREAM_IDLE_TIMEOUT_MS` | 60000 | Maximum wait between nonempty body chunks |
| `CCM_UPSTREAM_TOTAL_TIMEOUT_MS` | 1800000 | Entire logical model call, including attempts and backoff |
| `CCM_UPSTREAM_MAX_ATTEMPTS` | 3 | Initial attempt plus retries; 1 disables retries |
| `CCM_UPSTREAM_RETRY_BASE_MS` | 500 | Exponential backoff base, plus up to 50% jitter |

Millisecond values must be 1..86400000; attempts must be 1..10. Invalid values
fail startup. `CCM_EVALUATION_TIMEOUT_MS` is obsolete and ignored with a startup
warning. It does **not** override the new defaults, including when an old `.env`
still contains 30000. Role multipliers have been removed: Foreman, specialist,
generator, evaluator and composition calls share the independent budgets.

The total cap is per model call, not per multi-stage workflow or client request.
It begins after the first operator boundary; later retries/backoff and pauses do
not renew it. An explicit operator interrupt/restart begins a fresh logical call.
Body heartbeats count as activity but cannot extend the total cap. JSON fallback
bodies are subject to the same read/total budgets as SSE. Client forwarding is
bounded by the remaining total budget.

## Retry contract

- Retry connection/body transport failures, first-response/idle timeouts,
  premature EOF, truncated JSON, and HTTP 408/429/500/502/503/504. Recognized SSE
  `rate_limit_exceeded`, `server_error`, and `overloaded_error` codes are transient.
- Do not retry total-budget exhaustion, malformed complete JSON/SSE, invalid
  response/tool structures, other HTTP statuses, storage failures, or client
  disconnects. Unrecognized provider stream error codes fail explicitly.
- Respect `Retry-After` seconds or HTTP-date when longer than the local backoff.
  If the delay does not fit the remaining total budget, return the last failure.
- Automatic retries stop once **any** chunk is enqueued to the originating client,
  including role/reasoning/usage-only output. This is deliberately conservative;
  enqueueing is not proof of remote receipt.
- Internal failed partial text/reasoning/tool fragments stay in their attempt's
  trace but are discarded before retry. No tool or workflow stage is executed by
  this layer; only the uncompleted model call repeats. Provider generation/billing
  can repeat, since there is no provider-side idempotency guarantee.
- Cancellation drops the active request/backoff future. Steering remains active;
  next-attempt guidance is appended at the existing execution boundary.

## Protocol and evidence

SSE is framed by complete events, supports LF/CRLF/CR, multiline data fields,
comments and split UTF-8/network chunks. The body limit is 16 MiB and the event
limit is 1 MiB. SSE requires `[DONE]` and an explicit finish reason; JSON fallback
requires an assistant message and finish reason. A result must contain answer
text, complete function calls, or refusal text. Tool IDs must be nonempty and
unique, names nonempty, and argument strings valid JSON. Incomplete tool calls
cannot become executable successful results. No automatic JSON-repair prompt or
provider/model fallback is introduced.

Every network attempt has its own operation span, linked through `call_id` and
attempt number in `upstream.attempt.started`. Dashboard timeline events include
`upstream.response`, `upstream.attempt.completed|failed`, and `upstream.retry`.
Failed spans contain `error` plus structured `upstream` evidence: category, cause
chain, stage, status, content type, provider request ID (when supplied), elapsed
time, byte/chunk/event counts, first/last body activity and client-output state.
HTTP error bodies are not consumed; status and selected correlation headers are
recorded instead of arbitrary HTML/provider payloads. Credentials are not added
to diagnostics. Successful model output still follows the existing trace policy.

Workforce model-call counts increment for each attempt; assignments do not.
Historical error spans remain intact when a subsequent attempt succeeds.
Usage unavailable from failed generations remains unknown, not zero.

## Verification

See [verification commands and limits](TEST_RESULTS.md).
