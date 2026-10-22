# Foreman routing validation and recovery

Foreman validates routing decisions and applies bounded correction after malformed
output. These checks concern protocol correctness, not general model reasoning or
task-following quality.

## Provider enforcement

Provider definitions accept `structured_output: true` (default false). Enable it
only after verifying schema support in your provider. The setting is resolved from
Foreman's actual provider, including role overrides and frozen execution settings.

Foreman decision requests use:

```json
{
  "response_format": {
    "type": "json_schema",
    "json_schema": {
      "name": "foreman_decision",
      "strict": true,
      "schema": { "anyOf": ["generated decision branches"] }
    }
  }
}
```

The schema placeholder above illustrates the envelope, not a runnable schema.
Runtime branches contain current agent/workflow IDs, available internal tools and
their argument contracts, and allowed creation/personality operations. Extra fields
are rejected. The current union schema/dialect is verified on llama.cpp; enabling
the flag for a different provider requires checking its schema support.

The cloud provider is not opted in without verification. It still receives JSON
instructions and the same local validation/correction behavior. A provider rejecting
the format is reported as a provider error; there is no silent format downgrade or
provider/model switch.

## Validation and correction

- Parse the entire routing response as one JSON object (no prose/markdown wrappers).
- Validate against the same schema subset used for generation: decision/tool names,
  fields, types, required values, enum membership and nonempty required strings.
- Unavailable IDs or policy-disallowed operations are rejected before dispatch.
  Runtime authorization still rechecks immediately before an operation, including
  explicit-user-instruction requirements for persistent changes.
- Initial generation plus **at most two correction generations**. Corrections retain
  the original task and append a concise validation error; failed model text is not
  echoed into the prompt.
- All generations and their configured transport retries share **one Foreman
  total-time budget**. Cancellation drops the in-flight work. Transport attempts
  retain their existing retry limits, so three generations need not mean only three
  HTTP requests if transport retries also occur.
- Exhaustion returns a routing protocol error identifying provider/model; no action
  is fabricated from an invalid response. Trace events `foreman.validation` record
  generation attempt, validity and whether provider schema enforcement was enabled.

## Presentation and scope

Provisional routing reasoning and announcements are retained in dashboard traces,
not delivered to the client. Only reasoning from a locally validated decision is
released, once, under the normal personality/output rules. Operation announcements
still precede the operation. Later final composition remains streamed normally.

The routing response_format is not applied to specialist calls, final answers,
generator/evaluator outputs or the separate continuation output-contract check.
Client-specified formats retain their existing behavior. Schema validity is not
semantic correctness: LFM2 passed live routing but still failed an exact-answer
instruction in composition. No model reassignment was performed to conceal that.

See [TEST_RESULTS.md](TEST_RESULTS.md) for probes, regressions and live replay limits.
