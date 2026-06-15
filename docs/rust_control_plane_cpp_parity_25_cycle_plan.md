# Rust Client/Proxy/Metaserver/Nodeserver C++ Parity Plan

Goal: close the remaining Rust-native control-plane gaps versus C++ TemporalStore without adding
brpc or Thrift surfaces. Each cycle should plan, implement, test locally, update the gap audit, and
push.

## 25-Cycle Backlog

1. Bridge metaserver scheduler lifecycle tokens to nodeserver admin APIs. Done.
2. Execute scheduler load/unload tasks against remote nodeserver HTTP endpoints. Done.
3. Add metaserver task result reporting from nodeserver lifecycle responses. Done.
4. Add scheduler-driven finish-load validation using task id and generation. Done.
5. Persist expected node lifecycle tokens in metaserver scheduler snapshots.
6. Add nodeserver async load/reload/unload jobs with progress and cancellation.
7. Reject foreground writes during controlled reloading/unloading states.
8. Add proxy heartbeat application of metaserver serving policy transitions.
9. Add proxy route-cache invalidation from metaserver topology events.
10. Add proxy backend quarantine recovery probes.
11. Add client background MetaSyncer jitter/backoff worker handle.
12. Add client stale-table policy enforcement before network writes.
13. Add client route refresh from proxy/meta topology-version headers.
14. Add metaserver stuck-transition report for loading/reloading/unloading shards.
15. Add metaserver repair task creation for missing primaries and under-replicated shards.
16. Add cross-service move workflow: load target, update membership, unload source.
17. Add Raft-backed metaserver replay for scheduler lifecycle tokens.
18. Add multi-proxy stale-cache convergence harness.
19. Add multi-client background MetaSyncer scale harness.
20. Add nodeserver restart recovery of lifecycle token/transition state.
21. Add metaserver scheduler safety checks for frozen/dropped tables and servers.
22. Add proxy/client behavior under table freeze/unfreeze during writes.
23. Add failover test while load/reload/unload task is in flight.
24. Add control-plane Prometheus metrics for scheduler and lifecycle blockers.
25. Final local scale gate requiring client/proxy/meta/nodeserver convergence after move/failover.

## Cycle 1 Implemented

Cycle 1 exposes lifecycle tokens end to end at the admin surface:

- metaserver scheduler submit responses include a `lifecycle_token` for rebalance steps
- nodeserver accepts lifecycle tokens through direct and C++-style admin routes
- nodeserver lists installed lifecycle tokens for inspection
- lifecycle transitions already stamp scheduler task id and generation when matching tokens exist

The next cycle should turn these APIs into an actual scheduler executor that calls remote
nodeserver load/reload/unload routes.

## Cycle 2 Implemented

Cycle 2 adds the first applied metaserver-to-nodeserver scheduler executor:

- `POST /meta/scheduler/execute_next` peeks the next runnable deterministic scheduler task.
- Dry-run mode reports the remote node calls without mutating the scheduler queue.
- Applied `LoadTarget` execution installs the lifecycle token through
  `/ServerService/RequireLifecycleToken`, then calls `/ServerService/Load`.
- Applied `UnloadSource` execution installs the lifecycle token, then calls
  `/ServerService/Unload`.
- Applied execution advances the scheduler with `Ok` on node success and `RetryLater` on node
  request/status failure.
- Regression coverage validates dry-run queue preservation and token-before-load ordering against
  a fake nodeserver.

The next cycle should add richer task outcome persistence/reporting and then extend the same
executor shape to reload/freeze and membership-update workflows.

## Cycle 3 Implemented

Cycle 3 makes scheduler execution observable from metaserver:

- Applied `execute_next` fetches `/ServerService/GetLifecycle` after remote node execution.
- The execution response includes the full node lifecycle report when available.
- The response also extracts the scheduler-stamped shard transition by matching shard id,
  operation, scheduler task id, and scheduler generation.
- Metaserver keeps a bounded in-memory execution ledger for recent applied scheduler outcomes.
- `GET /meta/scheduler/executions` exposes task id, node address, final status, scheduler result,
  retry/backoff metadata, node calls, lifecycle token, and matched lifecycle state.
- Regression coverage validates the lifecycle fetch, matched transition, and execution ledger.

The next cycle should add scheduler-driven finish-load validation using task id/generation so
metaserver can reject stale completion callbacks during move workflows.

## Cycle 4 Implemented

Cycle 4 closes the stale finish-load callback gap:

- `LoadFinishRequest` now carries optional `scheduler_task_id` and `scheduler_generation` fields.
- Legacy finish-load callbacks without scheduler identity remain backward compatible.
- When scheduler identity is present, metaserver validates it against the recent scheduler
  execution ledger before applying `finish_load`.
- Validation requires a matching successful load execution for shard id, task id, generation, node
  address, and load version.
- Stale or mismatched scheduler finish-load callbacks fail closed before mutating topology.
- Regression coverage validates a scheduler-approved finish-load callback and a stale generation
  rejection.

The next cycle should persist expected node lifecycle tokens in scheduler snapshots so these
validation records can survive metaserver restart.
