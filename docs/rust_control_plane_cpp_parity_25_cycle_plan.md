# Rust Client/Proxy/Metaserver/Nodeserver C++ Parity Plan

Goal: close the remaining Rust-native control-plane gaps versus C++ TemporalStore without adding
brpc or Thrift surfaces. Each cycle should plan, implement, test locally, update the gap audit, and
push.

## 25-Cycle Backlog

1. Bridge metaserver scheduler lifecycle tokens to nodeserver admin APIs. Done.
2. Execute scheduler load/unload tasks against remote nodeserver HTTP endpoints. Done.
3. Add metaserver task result reporting from nodeserver lifecycle responses. Done.
4. Add scheduler-driven finish-load validation using task id and generation. Done.
5. Persist expected node lifecycle tokens in metaserver scheduler snapshots. Done.
6. Add nodeserver async load/reload/unload jobs with progress and cancellation. Done.
7. Reject foreground writes during controlled reloading/unloading states. Done.
8. Add proxy heartbeat application of metaserver serving policy transitions. Done.
9. Add proxy route-cache invalidation from metaserver topology events. Done in cycle 10.
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
24. Add control-plane Prometheus metrics for scheduler and lifecycle blockers. Done early in cycle 9.
25. Final local scale gate requiring client/proxy/meta/nodeserver convergence after move/failover.

## Cycle 9 Implemented

Cycle 9 pulls the control-plane Prometheus gap forward because C++ parity needs scrapeable
operations state across proxy and metaserver, not only the data-node:

- proxy exposes `GET /metrics` and `GET /ProxyService/Metrics`
- proxy metrics cover request counters, route-cache entries/events, backend/metaserver errors,
  serving mode, and drop percent
- metaserver exposes `GET /metrics` and `GET /MasterService/Metrics`
- metaserver metrics cover request counters, inventory, resource states, topology version,
  scheduler queue depth, and scheduler execution results
- Raft-backed metaserver metrics append existing metaserver Raft Prometheus output
- tests validate proxy policy/error counters and metaserver inventory/state/scheduler gauges

The next cycle should return to route-cache invalidation from metaserver topology events, then
client/proxy topology convergence under table changes.

## Cycle 10 Implemented

Cycle 10 closes the direct route-cache invalidation gap:

- client exposes a metaserver topology invalidation report for cached routes
- route-affecting topology events invalidate unknown-version direct shard routes and older cached
  routes
- opened table routes keep using the existing table topology refresh path after invalidation
- proxy execute/batch/table paths run the invalidation check before routing
- the check is best-effort so request execution is not blocked by older/fake metaserver surfaces
- regression coverage moves shard 1 from data-node A to data-node B and verifies the next proxy
  write lands on B without manual refresh

The next cycle should focus on table-change convergence under multi-proxy/multi-client load.

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

## Cycle 5 Implemented

Cycle 5 makes scheduler finish-load validation durable across metaserver restarts:

- Scheduler snapshot files now store a Rust-native durable envelope containing both pending tasks
  and the bounded scheduler execution ledger.
- The persisted execution ledger includes the lifecycle token and matched nodeserver lifecycle
  state needed to validate scheduler-tagged finish-load callbacks after restart.
- The metaserver still loads legacy task-only scheduler snapshots for rolling upgrade and local
  developer compatibility.
- Applied scheduler executions persist immediately after ledger update, while scheduler queue
  mutations continue to persist through the existing submit/run/restore paths.
- Regression coverage validates restored execution-token validation and legacy snapshot loading.

The next cycle should add nodeserver async load/reload/unload jobs with progress and cancellation
so the scheduler can coordinate long-running lifecycle transitions instead of treating every node
operation as a foreground HTTP call.

## Cycle 6 Implemented

Cycle 6 makes nodeserver lifecycle transitions first-class async jobs:

- Data-node runtime task kinds now include `Load`, `Reload`, and `Unload`.
- Async lifecycle jobs use the existing shard-affine foreground queue, preserving ordering with
  writes for the same shard.
- Job status output now returns typed load/reload/unload responses, including deadline and
  cancellation status.
- Direct routes `/async_load`, `/async_reload`, and `/async_unload` submit lifecycle jobs.
- C++-style aliases `/ServerService/AsyncLoad`, `/ServerService/AsyncReload`, and
  `/ServerService/AsyncUnload` expose the same job contract.
- Regression coverage validates async lifecycle completion, lifecycle report updates, queued
  lifecycle cancellation before execution, and the C++ async load alias.

The next cycle should reject foreground writes while a shard is in controlled loading, reloading,
or unloading state so lifecycle jobs become a true write-safety barrier instead of only queued
work.

## Cycle 7 Implemented

Cycle 7 turns data-node lifecycle transitions into a write-safety barrier:

- Data-node runtime now rejects foreground writes while a shard lifecycle state is `loading`,
  `reloading`, or `unloading`.
- The guard applies to sync execute, checked execute, batch execute, checked batch execute, and
  queued foreground execute jobs.
- Read commands remain allowed during lifecycle transitions.
- Server and C++-style ServerService write routes now use runtime guarded execution instead of
  bypassing the runtime through direct engine calls.
- Raft-backed server writes check the same lifecycle guard before proposing a local write.
- Guarded sync writes now mark dirty keys on success, improving storage dump/GC accounting for
  non-async foreground writes.
- Regression coverage validates sync, checked, batch, and queued write rejection while reads still
  succeed.

The next cycle should apply metaserver serving-policy transitions from proxy heartbeats so proxies
converge on freeze/safe-mode state without waiting for manual route refresh.

## Cycle 8 Implemented

Cycle 8 makes proxy serving policy converge from metaserver heartbeat responses:

- `ProxyHeartbeatResponse` now carries Rust-native serving policy fields: `serving_mode` and
  `drop_percent`, with serde defaults for compatibility.
- Metaserver maps normal proxies to `serving` and frozen/dropped proxies to `not_serving`.
- Frozen proxy heartbeat responses now include `not_serving` and mark the response as a policy
  change.
- Proxy heartbeat handling applies metaserver serving policy updates, including `resource_frozen`
  heartbeat responses.
- Proxy policy enforcement immediately rejects traffic after a frozen heartbeat drives local mode
  to `NotServing`.
- Regression coverage validates metaserver policy fields and proxy application of frozen
  heartbeat policy.

The next cycle should add proxy route-cache invalidation from metaserver topology events so proxy
routes converge after topology changes without waiting for backend failures or TTL expiry.
