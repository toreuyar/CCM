# Provider concurrency and automatic recovery

All inference calls pass through shared, process-wide admission control. Foreman,
specialists, generators, evaluators, workflow steps and improvement jobs using the
same provider ID share capacity and recovery state, even across exposed models or
saved request configurations.

## Configuration

```json
{
  "providers": {
    "local": {
      "base_url": "http://localhost:9292/v1",
      "max_concurrent_requests": 1,
      "recovery": {
        "cooldown_ms": 30000,
        "max_cooldown_ms": 300000
      }
    }
  }
}
```

- `max_concurrent_requests`: optional positive integer. Omitted/null means unlimited
  active calls. Zero is invalid. This limits **inference calls**, not whole tasks;
  separate multi-step tasks can alternate calls.
- `recovery.cooldown_ms`: positive initial cooldown; default **30 seconds**.
- `recovery.max_cooldown_ms`: exponential-backoff cap; default **5 minutes**. Must be
  at least the initial cooldown and at most 24 hours.
- Restart after changing configuration. Admission settings from the current server
  configuration take precedence over older request snapshots for that provider ID.

Each provider ID has its own gate. Two IDs pointing at the same endpoint are still
separate gates; use one provider definition when the endpoint has a shared limit.
Limits are per CCM process, not distributed across multiple CCM instances.

## Capacity and cancellation

A slot is held from immediately before HTTP dispatch until the complete response
body/stream is consumed, fails, or is cancelled. Retry backoff and cooldown waits do
not hold slots. A limit of one prevents foreground and background inference calls
from overlapping for that provider.

Queued waits participate in the existing total model-call budget, and remain
cancellable by disconnect or operator redirect. Queueing does not reset transport
retry limits or permit replay after client-visible output. Shutdown/cancellation
releases owned capacity automatically.

## Circuit recovery

Transport failures classified as transient and non-success HTTP responses open the
provider circuit immediately. This includes permanent HTTP errors such as 401/403:
the individual call still fails without automatic transport retry, but other jobs
cannot immediately bombard that provider. Local errors and client cancellation do
not themselves open a healthy circuit.

1. New calls wait while the circuit is open. Already active calls finish normally.
2. After cooldown and after existing active calls drain, exactly **one waiting real
   request** becomes the recovery probe. No synthetic inference request is sent.
3. A successful probe closes the circuit and releases waiting calls subject to the
   concurrency limit. A failed probe doubles cooldown up to the configured cap.
4. `Retry-After` is a lower bound, even when longer than the configured cap.
5. A cancelled probe releases its slot and leaves the circuit open for another
   initial cooldown. A stale success from a previously active request cannot close
   a circuit opened by another request's failure.

The existing improvement worker remains sequential. A job waiting for provider
admission can delay later jobs in that worker, including jobs for another provider.
If admission consumes the call budget, the improvement job is returned to `queued`
for a later pass. Actual provider-call retry exhaustion still marks that job failed;
completed or failed jobs are not blindly replayed. Other queued work observes the
shared cooldown. Foreground admission timeout is returned to the client.

Recovery state is in memory and resets on process restart. This is not a durable
outage schedule, job-priority system, or fallback-provider mechanism.

## Observability and verification

Call traces contain `provider.admission.wait`, `provider.admission.acquired`, and
`provider.cooldown` events. A waiting span does not mean HTTP dispatch has occurred.
Existing `upstream.retry` events retain per-call retry decisions.

Regression coverage includes full-stream slot ownership across resolved states,
cross-job cooldown after HTTP 401, cancelled queued clients sending no HTTP,
single-probe recovery, escalating cooldown, stale successes, cancelled probes,
provider isolation and current-config ownership across old snapshots.
