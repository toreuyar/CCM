# Models, providers and inherited configuration

The OpenAI-compatible endpoint exposes the IDs in `ccm.json` → `models`.
Send one as the request's `model`. Omitting `model` selects `default_model`;
unknown IDs are rejected (they are no longer silently ignored). Native dashboard
chat currently uses the default model; a dashboard model picker is not included.

## Connections and exposed models

Providers must speak the OpenAI chat-completions protocol. Configuration example:

```json
{
  "default_model": "deepseek-v4.1-flash",
  "provider": "ollama-cloud",
  "personality_transparency": "flexible",
  "providers": {
    "ollama-cloud": {
      "base_url": "https://ollama.com/v1",
      "api_key_env": "OLLAMA_CLOUD_API_KEY"
    },
    "llama-swap": { "base_url": "http://127.0.0.1:8000/v1" }
  },
  "models": {
    "deepseek-v4.1-flash": {
      "provider": "ollama-cloud",
      "model": "deepseek-v4.1-flash"
    },
    "Nemotron-3-Nano-4B": {
      "provider": "llama-swap",
      "model": "Nemotron-3-Nano-4B",
      "personality_transparency": "introvert",
      "upstream": { "idle_timeout_ms": 90000 }
    }
  }
}
```

The map key is the public model ID; the nested `model` is the inference provider's
actual model ID. They need not be equal. Credentials are referenced by environment
variable name, never embedded in JSON or saved in execution snapshots. Omit
`api_key_env` for an unauthenticated provider; no Authorization header is sent.
URLs cannot contain embedded credentials, query strings or fragments.
Providers also accept `max_concurrent_requests` and a `recovery` object. See
[provider admission](PROVIDER_ADMISSION.md) for shared limits, cooldown defaults,
single-probe recovery and cancellation behavior.
Provider `structured_output: true` opts Foreman routing into verified JSON Schema
generation; it defaults to false and must be enabled only after checking your provider. See
[FOREMAN_ROUTING.md](FOREMAN_ROUTING.md) for schema dialect, validation and recovery.

## Inheritance

Model operational fields override global file fields, which override legacy
environment settings where supported, then built-in defaults. Nested timeout
fields inherit individually. Supported operational fields are:

- `personality_transparency`, `creation_policy`
- `creation_prompt`, `consolidation_prompt` (paths relative to the config file)
- `max_iterations`, `debug_enabled`
- `upstream`: `connect_timeout_ms`, `first_response_timeout_ms`,
  `idle_timeout_ms`, `total_timeout_ms`, `max_attempts`, `retry_base_ms`

`provider` selects a named connection. At global level `inference_model` sets the
inference default; inside a model or agent override the equivalent field is `model`.
Database path, listener and memory budget remain process-wide. Do not place them
inside a model. Definitions are workforce-scoped; public model aliases do not create separate workforces.
File changes require restart; personality switches in flexible mode are still live
from the next request and now scoped to **client/API key + exposed model**.
Creation-policy saved overrides use that same scope and retain their existing
precedence over the configured creation-policy default.

Legacy `CCM_MODEL`, `CCM_UPSTREAM_BASE_URL`, and `OLLAMA_CLOUD_API_KEY` remain
supported when no explicit provider/model map is supplied. Multiple exposed models
require a configured default unless the legacy default is one of the listed IDs.

## Agent and built-in role overrides

An `agents` map can be global or inside a model definition. Keys are agent UUIDs
or names; built-in role IDs are `foreman`, `agent-generator`, `workflow-generator`
and `evaluator`. Foreman's setting applies to routing and final composition.

Example fragment (an illustration, not this deployment's chosen role assignment):

```json
{
  "agents": {
    "evaluator": {
      "provider": "ollama-cloud",
      "model": "deepseek-v4.1-flash",
      "upstream": { "idle_timeout_ms": 90000 }
    }
  }
}
```

These per-agent entries support `provider`, `model` and `upstream` budgets. Whole-
request personality/creation/debug/iteration settings belong at model/global level.
Model-specific agent overrides layer over global agent overrides; UUID wins over
name within each layer. Explicit administrator overrides take precedence over a
generated definition's assignment. Otherwise an agent's stored provider/model
override is used; empty fields inherit the selected exposed model's resolution.

## Separate assignment catalog for creation/editing

`agent_models` is **not** the exposed model list. It supplies the allowed choices
and administrator-authored selection guidance to Foreman, agent/workflow generators
and the evaluator. It can be global or replaced for an exposed model. Each entry:

```json
{
  "agent_models": {
    "choice-id": {
      "provider": "configured-provider-id",
      "model": "actual-inference-model-id",
      "context_tokens": 32000,
      "capabilities": ["administrator-described capability"],
      "reasoning": "Administrator-provided reasons to choose this model."
    }
  }
}
```

This is a schema illustration, not an asserted capability or recommended assignment.
Context sizes and descriptions guide model selection; they are not tokenizer-based
context enforcement or independently verified benchmarks.

Foreman specifies a catalog choice and reason in creation requirements. The
dedicated generator authors/registers the agent, returning
`model_choice: {"catalog_id":"choice-id","reason":"why it fits"}`. Evaluator may
return that field in an agent replacement to reassign based on evidence. The runtime
rejects unknown choices, missing reasons and arbitrary provider/model fields in
generated definitions. Omitted choice preserves assignment on revision.

The separate `generated_agent_default` object accepts optional `provider` and
`model`. Without a catalog choice, these fields seed new agent assignments; omitted
fields inherit the selected model. `{}` means full inheritance. It can be overridden
field-by-field inside an exposed model. The portable examples use `{}` and an empty
`agent_models` catalog. This is a supported configuration, not a pending requirement;
exposed client models work independently and require no assignment-catalog rationale.

## Persistence, migration and continuation safety

- One-time migration clears old server-populated live agent model values to inherit;
  historical definition snapshots and pending execution snapshots are retained.
- Existing client-only personality/creation preferences migrate to the default
  exposed model. Other models start with their configured defaults.
- Resolved request and per-actor settings are snapshotted without credential values.
  Tool continuations cannot change exposed model/client and retain the originating
  settings. Automatic improvement restores the originating model configuration.
- Credentials are resolved from the configured environment reference on recovery.
  Missing credentials fail explicitly; there is no fallback to another provider.
- Wire responses report the exposed ID; provider requests use the actual inference
  ID. `model.resolved` trace events record provider/model attribution for each call.
- All state uses the existing asynchronous DB publication: abrupt crashes can lose
  changes newer than the last published snapshot.
