# Claim-to-code evidence appendix

Companion to [CCM.md](CCM.md), author-approved manuscript version 1.0. Paths are
relative to the repository root. Symbols are navigation anchors, not stable APIs. The
accompanying `source-manifest.json` identifies the packaged implementation files by
SHA-256. The paper's engineering results retain their stated 10 October 2026
evidence cutoff; current file hashes do not imply a new benchmark.

| Paper claim | Implementation evidence | Verification and limits |
|---|---|---|
| Chat API and model listing exist | [`src/main.rs`](../../src/main.rs), route registration; [`src/handlers.rs`](../../src/handlers.rs), chat/model handlers | `scenarios.rs`, `scenarios_models.rs`; not universal OpenAI compatibility |
| Foreman delegates and composes | [`src/foreman.rs`](../../src/foreman.rs), `foreman_call_tracked`, composition contract; [`src/runtime.rs`](../../src/runtime.rs), `dispatch` | `scenarios_contracts.rs`, `scenarios_transparency.rs`; composition quality is not guaranteed |
| Seven provisions are required | [`src/meta.rs`](../../src/meta.rs), `PROVISIONS`, `parse_agent` | Nonempty-field checks are structural, not a semantic-quality proof |
| Creation uses dedicated specialists and registration | [`src/meta.rs`](../../src/meta.rs), roster/create functions; [`src/registry.rs`](../../src/registry.rs), `create` | `scenarios_policy.rs`; consolidation/reuse is separate from arbitrary prompt mutation |
| Automatic creation timing is policy-controlled | [`src/policy.rs`](../../src/policy.rs), policy context and tools | `scenarios_policy.rs`; does not establish every proposed explicit management feature |
| Evaluator owns verdicts | [`src/meta.rs`](../../src/meta.rs), evaluator prompt/evaluation; [`src/registry.rs`](../../src/registry.rs) | `scenarios_improvement.rs`; verdict correctness is still model-dependent |
| Workflows have one current stage | [`src/workflow.rs`](../../src/workflow.rs), `Workflow`, `Stage`; [`src/runtime.rs`](../../src/runtime.rs), `new_run`, `run_workflow` | Branch/cycle/continuation scenarios; no parallel fan-out/join executor |
| Branches use ordered exact JSON equality | [`src/workflow.rs`](../../src/workflow.rs), `transition` | `workflow::tests`; malformed branch reports fail |
| Workflow execution is bounded | [`src/workflow.rs`](../../src/workflow.rs), `validate`; [`src/runtime.rs`](../../src/runtime.rs), step/tool-round checks | `workflow_cycle_stops_at_budget` and related scenarios; bounded execution is not guaranteed task success |
| Client tools suspend/resume execution | [`src/runtime.rs`](../../src/runtime.rs), pending-result checks, encoded calls and persisted runs; [`src/loop_runner.rs`](../../src/loop_runner.rs) | Client-tool effects occur at the client; current embedded native tools are a separate permission-controlled path |
| Models and agents inherit configuration | [`src/model_config.rs`](../../src/model_config.rs), `select_model`, `for_agent`, `Overrides::apply`; [`src/config.rs`](../../src/config.rs) | `scenarios_models.rs`; distinguish exposed model IDs from actual inference model IDs |
| Catalog-guided assignments are optional | [`src/model_config.rs`](../../src/model_config.rs), `agent_model_contract`, `assign_generated` | Catalog validation tests; capability/context metadata are administrator descriptions |
| Foreman JSON enforcement is provider-scoped | [`src/foreman.rs`](../../src/foreman.rs); [`src/foreman_protocol.rs`](../../src/foreman_protocol.rs), `schema`, `validate` | `scenarios_foreman_protocol.rs`; not a universal structured-output layer |
| Recovery and capacity are shared across calls | [`src/provider_gate.rs`](../../src/provider_gate.rs), `Registry`, `Gate::acquire`, `Lease`; [`src/upstream.rs`](../../src/upstream.rs) | Gate tests and `scenarios_upstream.rs`; per-process/provider-ID scope, not distributed admission |
| Retries are bounded and delivery-aware | [`src/upstream.rs`](../../src/upstream.rs), retry loop; [`src/upstream_transport.rs`](../../src/upstream_transport.rs) | Reliability scenarios; no replay after client-visible output |
| Automatic improvement follows publication | [`src/improvement.rs`](../../src/improvement.rs), enqueue/release/process functions; [`src/handlers.rs`](../../src/handlers.rs) | `scenarios_improvement.rs`; response completion is server-observed, not client acknowledgement |
| Improvement waiting differs from job failure | [`src/improvement.rs`](../../src/improvement.rs), `process_one` | Admission timeout returns job to queue; provider retry exhaustion still fails it |
| Evaluation evidence is bounded | [`src/improvement.rs`](../../src/improvement.rs), conversation checkpoints and job capture; [`src/brief.rs`](../../src/brief.rs), `summarize_request` | The paper's request-scoped baseline is extended by conversation checkpoints in the packaged runtime; neither provides exhaustive evidence |
| Definitions and continuations are version-sensitive | [`src/runtime.rs`](../../src/runtime.rs), execution snapshots; [`src/registry.rs`](../../src/registry.rs), guarded mutations | Version, retirement and reset regressions; external tools are not rollbackable |
| Personality governs presentation | [`src/transparency.rs`](../../src/transparency.rs); [`src/config.rs`](../../src/config.rs), `PersonalityTransparency` | `scenarios_transparency.rs`; reasoning output is not verified internal cognition |
| Persistence is memory-first and asynchronous | [`src/storage.rs`](../../src/storage.rs); [`src/dashboard_cache.rs`](../../src/dashboard_cache.rs) | Storage/replay/cache tests; crash loss of unpublished changes is explicitly accepted |
| MCP, supervised async delegation and broader evaluation were planned at the paper cutoff | [`src/mcp_server.rs`](../../src/mcp_server.rs), [`src/async_ops.rs`](../../src/async_ops.rs), [`src/improvement.rs`](../../src/improvement.rs) | These capabilities are implemented in the packaged runtime; use current operational documentation rather than the paper's dated status table |

## Evidence classification

- **Source evidence:** establishes the inspected implementation's contract and code
  paths, not empirical quality or universal environmental behavior.
- **Controlled regressions:** exercise specified conditions with synthetic providers
  and data. Passing counts are not scientific performance measures.
- **Operational probes:** establish a result for a particular deployed stack and
  request; they do not establish general capability or comparative advantage.
- **Planned extensions:** architectural intentions, explicitly excluded from claims
  about the currently implemented runtime.

The test total cited in the paper is the recorded provider-admission verification
result (186 Rust tests, four frontend tests), not a fresh benchmark conducted for
this manuscript. This version did not rerun tests or update historical counts.
Local deployment logs and private operational notes are not included in the paper
package. Public artifacts contain no credentials, client conversations, private URLs,
or raw traffic exports.
