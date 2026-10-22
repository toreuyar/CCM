# Memory-first execution and presentation

CCM uses memory-first delivery, independent asynchronous persistence and bounded
dashboard caching. A crash can lose unpersisted data. Configuration:

```json
{
  "memory": { "budget_mib": 256 }
}
```

The default is **256 MiB**. Implementation validation accepts integer values from
32 through 16384. Like other server configuration, changes take effect at startup.
This is the combined **cache/buffer allocation budget**, not a total process RSS
limit. Request/model collectors, transient HTTP/JSON results, SQL transaction/sort
workspace and runtime overhead are outside that cache budget.

## Execution and persistence

The existing transactional SQL model runs through a bounded memory-backed SQLite
VFS. This covers execution/control state, definitions, continuations, jobs and
traces, rather than moving only token logs off disk. Incoming trace/body data is
visible in memory immediately. Disk is a delayed persistence layer.

Committed page versions are immutable and shared. A dedicated OS thread batches
commits over a 250ms window and publishes a complete SQLite file by atomic rename
after syncing it. Reflinks avoid copying unchanged data where supported; other
filesystems use a full file copy on that worker. Directory synchronization follows
publication. A failed publication retains the previous complete snapshot and
reports an error through `/api/memory`; subsequent writes fail rather than silently
pretending persistence is healthy. Recent memory commits can be lost on a crash.

The backing `portfolio.db` remains a standard SQLite database. External readers see
the last published snapshot, not necessarily current live state. CCM owns writes;
direct external modification while running is unsupported. A `.ccm-lock` sidecar
excludes a second CCM writer; `.ccm-next` is the unpublished snapshot. Startup
recovers legacy SQLite journals before using the memory layer. Graceful termination
drains HTTP requests and flushes committed database state and the traffic journal.

Clean inactive pages are evicted first. Older data can be loaded from the backing
snapshot. Only exhausted capacity that cannot safely be reclaimed waits for pending
persistence/readers. A single transaction larger than the available page budget
fails explicitly; it cannot grow the cache without bound.

## Independent consumers

- **Client:** an independent relay drains provider output into a byte-budgeted
  memory queue. Network/client pacing does not stop ingestion until that queue's
  capacity is exhausted. Disconnect cancellation, tool handoff ordering and
  response-gated jobs are retained. Enqueued output remains conservatively treated
  as exposed for retry suppression.
- **Dashboard:** regular GET results use a bounded serialized response cache with
  100ms freshness. Mutating dashboard requests invalidate it. Cold queries run on
  blocking workers against immutable memory snapshots, without holding the
  execution connection lock during the query. Older pages load asynchronously and
  warm the shared cache when their backing version is still current. At most four
  dashboard/event-read jobs run concurrently. Live cursor pages bypass response
  caching; SSE consumers read independently and release snapshots before network
  delivery.
- **Traffic journal:** a dedicated worker writes buffered JSONL records and flushes
  at 100ms intervals. Queued records are byte-budgeted. Disk errors are logged;
  produced frames still do not prove remote receipt.

The configured budget reserves half for database pages/read snapshots (including
1 MiB for the execution SQLite pager), one quarter for client delivery, and one
eighth each for dashboard responses and journal buffers. Charges include conservative
metadata allowances. Each pool is bounded independently; exhaustion of one pool
does not borrow the others' reservations.

## Recovery and visibility

Async persistence applies to operational state too. A response may reach a client
before its final state, continuation or improvement job is on disk. Crash recovery
uses only the last published transaction prefix; acknowledged-but-unpersisted work
can be lost or repeated. Existing receipt/lineage guarantees apply to that persisted
prefix, not to all memory-visible commits.

Dashboard SSE IDs contain a process epoch and sequence. After restart, a previous
epoch (or an ahead-of-head legacy cursor) emits `trace_reset`; the native dashboard
reloads snapshots instead of confusing reused sequence numbers with old events.
Legacy numeric Last-Event-ID values remain accepted within their available history.

`/api/memory` exposes configured budget, cache/queue usage, dashboard hits/misses,
commit and persisted generations, pending pages and persistence errors. Provider
attempt diagnostics additionally expose `local_processing_us` and
`client_enqueue_wait_us`. Local processing includes parsing, trace/presentation
work and enqueue waits; provider waiting is separate. Local backpressure does not
consume the next provider inactivity interval; the total call cap still includes it.

See [TEST_RESULTS.md](TEST_RESULTS.md) for scripted throughput, recovery and live
deployment evidence. Scripted speedups are not a claim about every real model call.
