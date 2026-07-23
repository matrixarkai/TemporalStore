# Secondary Read Latency Investigation - 2026-05-31

## Summary

Scale testing showed secondary/replica-eligible reads much slower than primary reads even with a shared `file://` storage path. The main reason is that the secondary does not serve directly from the shared file as if it were primary memory. It has to poll/refresh primary metadata, restore page/index/oplog metadata, replay oplog into its local object manager, replay index logs, and then serve from its own local object/page state. If a read reaches a secondary before that slot/object is visible, the benchmark retries and includes the wait time in the measured latency.

## Code Change

Changed the replicator loop so it does not sleep while it is actively replaying logs or while it already knows it needs another remote metadata refresh.

Files:

- `src/partition/storage/replicator.cc`
- `src/partition/storage/replicator.h`

This reduces avoidable catch-up delay in the async secondary replay path.

## Test Run

Release build was rebuilt in WSL and tested with:

- 3 metaservers
- 1 primary server
- 2 secondary servers
- shared `file://` storage
- proxy smoke test included
- string workload: 8,000 ops, 8 threads
- sequence workload: 6 keys, 1,000 rows/key, 800 query ops/case, 8 threads

The WSL `/tmp` result directory was cleaned after the run, but the captured output showed:

Before this change, string replica-eligible GET was roughly:

- avg: 40.4 ms
- p50: 46.0 ms
- p99: 100 ms

After this change:

- avg: 17.6 ms
- p50: 4.0 ms
- p99: 61.8 ms

So the simple KV secondary path improved materially, but it is still slower than primary because it is async/catch-up based.

## Remaining Issue

Feature sequence replica-eligible queries still emitted many first-attempt failures:

```text
FEATURE Query failed: NotFound: Key not exists due to slot not found
```

The benchmark retries until success, so final errors remain zero, but the retry wait is counted inside query latency. That means the high sequence secondary latency is mostly visibility/catch-up lag, not raw shared-file read speed.

## Next Fixes

1. Add replica readiness metrics/API: expose `oplog_gap`, `index_log_gap`, replay lag, and per-partition readiness so tests and clients know whether a secondary is safe to read.
2. Add lag-aware routing: avoid routing replica-eligible reads to secondaries whose replay gap is non-zero or whose slot is not visible yet.
3. Add optional primary fallback for strict online reads: if a secondary returns `slot not found` or is behind the requested consistency point, retry primary immediately instead of waiting in 20 ms retry sleeps.
4. Split benchmark metrics: report first-attempt success, retry count, fallback count, and final latency separately.
5. For sequence/risk workloads, run a secondary warm-up/catch-up barrier before measuring steady-state secondary query latency.

