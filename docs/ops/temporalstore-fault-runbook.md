# TemporalStore Fault Runbook

This runbook covers the Rust production fault alerts that are backed by current
Prometheus and readiness surfaces. It is intentionally scoped to the local Rust
service model; external host chaos, full OpenRaft/raft-rs replacement, and
cloud-scale validation remain separate production readiness gates.

## Common Inputs

- Proxy metrics: `GET /metrics`
- Proxy readiness: `GET /readiness` or `GET /cpp_parity`
- Raft metrics from data or meta runtimes:
  - `temporalstore_raft_cluster_has_majority`
  - `temporalstore_raft_cluster_commit_index`
  - `temporalstore_raft_node_commit_index{role="..."}`
  - `temporalstore_raft_node_lag`
  - `temporalstore_raft_node_apply_lag`
- Production readiness metrics:
  - `temporalstore_production_readiness_ready`
  - `temporalstore_production_readiness_blockers`
  - `temporalstore_production_readiness_service_ready`
  - `temporalstore_production_readiness_service_blockers`

## Stuck Replica

Symptoms:
- `temporalstore_raft_node_lag` stays above the alert threshold.
- Replica reads fail with a lagging-replica error.
- Readiness reports data-node or Raft blockers.

Immediate actions:
- Keep writes on the current leader only.
- Do not transfer leadership to the lagging node.
- Check the node's WAL directory, page-store directory, and process logs for
  fsync, checksum, snapshot, or page read errors.
- If the follower is only behind, let catch-up run. If lag keeps growing, remove
  the follower from voters before restarting it.

Recovery checks:
- `temporalstore_raft_node_lag{node_id="..."}` returns to `0`.
- `temporalstore_raft_node_apply_lag{node_id="..."}` returns to `0`.
- Replica read succeeds after catch-up.
- `temporalstore_production_readiness_service_ready{service="data_node"}` is
  not newly degraded by the incident.

## Split-Brain Risk

Symptoms:
- `TemporalStoreRaftSplitBrainRisk` fires because more than one leader is
  visible for the same `kind`.
- Majority state disagrees across process views.
- Clients see conflicting leader or stale-read errors.

Immediate actions:
- Freeze writes at proxy admission if possible.
- Prefer the side with current majority and highest committed index.
- Isolate stale leaders from clients and peers.
- Do not manually copy page or WAL files between nodes.

Recovery checks:
- Exactly one `temporalstore_raft_node_commit_index{role="leader"}` series remains per group.
- `temporalstore_raft_cluster_has_majority` is `1`.
- Commit indexes converge across live voters.
- A fresh write commits and follower reads return the same value after catch-up.

## Slow Follower Or Apply Loop

Symptoms:
- `temporalstore_raft_node_lag` or `temporalstore_raft_node_apply_lag` is high.
- Snapshot transfer is repeatedly attempted.
- Foreground queue or storage readiness reports become degraded.

Immediate actions:
- Check disk latency and free space on the follower.
- Check whether page-store reads or cache refill failures are increasing.
- Prefer snapshot bootstrap for a very stale follower.
- Avoid leader transfer until apply lag is below the alert threshold.

Recovery checks:
- Lag and apply lag both return to `0`.
- No stale snapshot rejection is repeating.
- Storage recovery report has no unreadable page refs, corrupt segments, or
  owner mismatches.

## Disk Or Storage Pressure

Symptoms:
- `TemporalStoreStorageCacheBlockers` fires.
- Storage readiness reports dirty-slot, stale-extent, unreadable-page, or
  owner-mismatch blockers.
- Cache memory or disk bytes keep increasing while hit rate drops.

Immediate actions:
- Inspect the storage production readiness report for the affected shard.
- Run lifecycle planning in dry-run mode first; apply dump, compaction, GC, and
  cache invalidation only when the report shows no follower-cursor retention
  conflict.
- Refuse page GC while checkpoint, page, or oplog data is still needed by a
  known follower cursor.

Recovery checks:
- `storage_cache` readiness blockers decrease.
- Stale-extent and dirty-slot counts return below policy thresholds.
- `all_live_pages_readable` is true in the storage recovery report.
- Follower replay cursors still resume without oplog gaps.

## Production Readiness Blocked

Symptoms:
- `temporalstore_production_readiness_ready == 0`.
- `temporalstore_production_readiness_blockers > 0`.

Immediate actions:
- Break down blockers by `area` and by `service`.
- Treat readiness blockers as release blockers unless the deployment explicitly
  opts into local-model validation only.
- For local-model-only validation, record the exact failed capabilities in the
  release notes.

Recovery checks:
- The relevant per-area blocker count decreases after the fix.
- The service gate report moves to a less severe state.
- The readiness JSON and Prometheus metrics agree on blocker counts.

## Ops And Scale Gate

Use this gate before marking deployment ops or scale testing evidence complete:

```bash
tools/run_ops_scale_readiness.sh
tools/run_ops_scale_readiness.sh --run-local-scale
tools/run_ops_scale_readiness.sh --run-distributed-raft
```

The first command validates the production evidence contract: autoscale and
metaserver-driven rebalance surfaces, dashboard and alert files, tracing and
non-Raft auth/TLS runbook, Docker/local scale harness, distributed Raft load
harness, and unified C++/Rust workload corpus. The optional flags execute the
local scale and real-process distributed Raft harnesses.

Recovery checks:
- The ops/scale readiness JSON reports `production_ready: true`.
- Local scale reports `replication_healthy: true` and `max_replica_lag: 0`.
- Distributed Raft reports successful follower reads, leader transfer,
  membership scale up/down, snapshot bootstrap, and apply health.
