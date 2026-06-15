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
11. Add client background MetaSyncer jitter/backoff worker handle. Done in cycle 11.
12. Add client stale-table policy enforcement before network writes. Done in cycle 12.
13. Add client route refresh from proxy/meta topology-version headers.
14. Add metaserver stuck-transition report for loading/reloading/unloading shards.
15. Add metaserver repair task creation for missing primaries and under-replicated shards.
16. Add cross-service move workflow: load target, update membership, unload source. Unload
    busy-safety sub-slice done in cycle 13; scheduler retry classification done in cycle 14.
17. Add Raft-backed metaserver replay for scheduler lifecycle tokens. Controlled reload
    scheduler step done in cycle 15; load/reload/unload workflow harness done in cycle 16;
    raft-backed workflow route coverage done in cycle 17.
18. Add multi-proxy stale-cache convergence harness. Nodeserver-disappears scheduler retry
    failure pass done in cycle 18.
19. Add multi-client background MetaSyncer scale harness. Disappeared-node retry across
    load/reload/unload and retry snapshot restore done in cycle 19.
20. Add nodeserver restart recovery of lifecycle token/transition state. Runtime lifecycle
    snapshot/restore API and tests done in cycle 20.
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

## Cycle 11 Implemented

Cycle 11 makes the client MetaSyncer closer to the C++ background worker shape:

- added `ClientMetaSyncLoopOptions`
- added a managed `ClientMetaSyncLoopHandle` with stop and stop/join
- added one-shot due-table sync for tests and future control-plane integration
- the background worker honors each table's `next_sync_after_unix_ms`
- sync errors now use bounded retry backoff from `topo_error_retry_interval_ms`
- existing `start_meta_sync_loop(interval_ms)` remains compatible
- regression coverage validates existing table handles update from background sync and the worker
  stops cleanly

The next cycle should focus on multi-proxy/multi-client table-change convergence and then stale
table policy enforcement before network writes.

## Cycle 12 Implemented

Cycle 12 adds stale-table write policy enforcement:

- table writes check whether the table's MetaSync state is due before selecting a shard
- due write paths synchronously refresh table topology through metaserver
- batched table writes run the same guard before grouping/routing
- read paths remain cache-tolerant
- regression coverage changes a table from shard 10 to shard 20 and verifies the next write routes
  to shard 20 before network execution

The next cycle should focus on multi-proxy/multi-client table-change convergence and table
freeze/unfreeze write behavior.

## Cycle 13 Implemented

Cycle 13 tightens the nodeserver unload side of controlled move safety:

- Direct `unload_shard_with` now checks the shard-affine data-node lane before changing local
  state.
- If foreground or background work is queued/running for the shard, direct unload returns
  `shard_busy`, records a failed unload lifecycle transition, and leaves the shard loaded.
- Queued unload jobs keep the existing C++-style scheduler behavior: they wait behind prior
  shard work and then unload once the lane is clear.
- Regression coverage validates both direct busy rejection and queued unload serialization.
- Local scale validation passed with 4 initial raft nodes, scale-out to 5 nodes, 3 failovers,
  shared-store comparison enabled, and max replica lag 0.

The next cycle should wire the same busy-safety signal into the metaserver scheduler executor so
move/unload tasks retry instead of racing active shard work.

## Cycle 14 Implemented

Cycle 14 wires nodeserver unload busy-safety into metaserver scheduler execution:

- Metaserver scheduler execution now classifies node responses through an explicit retry/abort
  helper instead of treating every non-ok response the same.
- Operational node-admission statuses, including `shard_busy`, queue pressure, timeout,
  unavailable, loading/unloading, and leader-not-ready statuses, map to `RetryLater`.
- Local scheduler/request mismatches still map to `Aborted`, preserving fail-fast behavior for
  bad control-plane input.
- A busy unload from `/ServerService/Unload` now leaves the scheduler task queued with retry
  backoff and records the retry in the execution ledger.
- Regression coverage validates `shard_busy` unload execution, lifecycle fetch, execution record,
  retry count, next run time, and preserved queue length.
- Local scale validation passed with 4 initial raft nodes, scale-out to 5 nodes, 3 failovers,
  shared-store comparison enabled, and max replica lag 0.

The next cycle should use this retry signal in a cross-service move harness that performs load,
membership update, busy unload retry, and eventual unload convergence.

## Cycle 15 Implemented

Cycle 15 adds controlled reload as a first-class scheduler lifecycle step:

- `RebalanceStep::ReloadTarget` now carries shard, replica, node, and load version metadata.
- Scheduler lifecycle tokens now distinguish `reload` from `load` and carry the reload
  load-version/generation for nodeserver validation.
- Metaserver scheduler execution installs the reload lifecycle token, calls
  `/ServerService/Reload`, fetches `/ServerService/GetLifecycle`, and records the matched reload
  lifecycle state in the execution ledger.
- Request mismatch and missing reload request paths reuse the same fail-fast validation as
  controlled load.
- Regression coverage validates reload token issuance, node call ordering, local node id
  injection, readonly reload lifecycle state matching, scheduler report success, and execution
  ledger recording.

The next cycle should add a cross-service harness that exercises load -> reload -> unload under
metaserver scheduling, then replay the same scheduler mutations through raft-backed meta.

## Cycle 16 Implemented

Cycle 16 adds a scheduler-driven lifecycle workflow harness:

- A stateful fake nodeserver now accepts scheduler lifecycle tokens, applies load, reload, and
  unload calls, and reports the latest lifecycle transition through `/ServerService/GetLifecycle`.
- The metaserver scheduler test submits and executes `LoadTarget`, `ReloadTarget`, and
  `UnloadSource` tasks in sequence against the same node surface.
- The workflow validates token installation before each node action, `local_node_id` injection
  for load/reload, lifecycle-state matching for serving/readonly/unloaded states, queue drain
  after each task, and execution-ledger ordering.
- This closes the first local cross-service harness gap for controlled load -> reload -> unload.

The next cycle should replay the same scheduler workflow through the raft-backed metaserver path
and then add a failure pass where a node disappears during a lifecycle step.

## Cycle 17 Implemented

Cycle 17 replays the scheduler lifecycle workflow through the raft-backed metaserver route layer:

- A `MetaBackend::Raft` test now drives `LoadTarget`, `ReloadTarget`, and `UnloadSource` through
  the same `/meta/scheduler/submit` and `/meta/scheduler/execute_next` handler paths.
- The workflow validates lifecycle token installation, nodeserver load/reload/unload calls,
  lifecycle fetches, execution-ledger ordering, and drained scheduler queue while the metaserver
  backend is a `ProductionMetaRaftRuntime`.
- This gives raft-backed route coverage for the controlled lifecycle workflow before adding
  destructive node-failure cases.

The next cycle should add a failure pass where the nodeserver disappears during load/reload/unload
and the scheduler records retryable execution state instead of losing the task.

## Cycle 18 Implemented

Cycle 18 adds the first destructive scheduler failure pass:

- A metaserver scheduler test now submits a controlled `LoadTarget` task and executes it against
  a reserved-but-unserved loopback address to simulate a disappeared nodeserver.
- The failure path records `node_request_failed`, fetches lifecycle as best-effort, maps the
  execution to `RetryLater`, increments retry count, sets deterministic backoff, and leaves the
  scheduler task queued.
- The execution ledger records the retryable failure without a lifecycle state, preserving
  operator visibility and future retry/replay state.

The next cycle should extend the disappeared-node pass across reload and unload, then persist the
retry state through scheduler snapshot/restart.

## Cycle 19 Implemented

Cycle 19 expands the disappeared-nodeserver failure pass:

- The scheduler retry test now covers controlled `load`, `reload`, and `unload` tasks against an
  unserved loopback address.
- Each lifecycle operation verifies `node_request_failed`, best-effort lifecycle fetch failure,
  `RetryLater`, retry count, deterministic next-run time, preserved queue length, and execution
  ledger state.
- A scheduler snapshot/restart regression verifies retry state and the execution ledger survive
  restore after a disappeared-node retry.

The next cycle should move from metaserver scheduler failure coverage into data-node lifecycle
token/transition persistence across restart.

## Cycle 20 Implemented

Cycle 20 adds Rust-native data-node lifecycle snapshot/restore:

- `DataNodeLifecycleSnapshot` captures lifecycle transitions and installed scheduler lifecycle
  tokens with a format version.
- `DataNodeRuntime::lifecycle_snapshot` exports sorted transitions and tokens for durable storage
  by the server layer.
- `DataNodeRuntime::restore_lifecycle_snapshot` restores transitions and tokens, rejects unknown
  snapshot versions, and preserves scheduler token enforcement after restore.
- Regression coverage validates five restart-readiness checks: snapshot export, transition
  restore, token restore, restored-token enforcement, and bad-format rejection.

The next cycle should wire this snapshot API to nodeserver file-backed startup/shutdown routes so
the runtime recovery contract is exercised through the process surface, then add a restart harness.

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

## Cycle 9 Implemented

Cycle 9 exposes durable data-node lifecycle snapshots through the nodeserver compatibility surface:

- REST routes now export, restore, save, and load data-node lifecycle snapshots under
  `/server/lifecycle/snapshot`.
- C++-style `ServerService` aliases expose the same lifecycle snapshot operations for scheduler,
  proxy, and metaserver orchestration tests.
- File-backed save/load persists scheduler lifecycle tokens and shard lifecycle transitions so a
  restarted node can recover controlled load/reload/unload state before accepting scheduler
  callbacks.
- Route-level regression coverage validates snapshot export, direct restore, file save/load,
  scheduler token recovery, transition recovery, and unsupported snapshot-version rejection.

The next cycle should use these snapshot routes from a process-level nodeserver restart harness so
metaserver-directed load/reload/unload workflows prove recovery through the real HTTP boundary.

## Cycle 10 Implemented

Cycle 10 validates data-node lifecycle snapshot recovery through an HTTP restart boundary:

- A test-only nodeserver HTTP harness now exposes the same C++-style `ServerService` lifecycle
  snapshot routes used by orchestration clients.
- The restart scenario saves scheduler tokens and lifecycle transitions from one HTTP server
  instance, starts a second runtime, loads the snapshot through HTTP, and verifies the restored
  state through `GetLifecycleSnapshot`.
- The recovered reload path proves the restored scheduler token is consumed by the restarted
  node, preserving scheduler task id and generation metadata in the lifecycle report.
- The scenario covers five parity checks: HTTP save, durable file presence, HTTP load, restored
  token/transition inspection, and scheduler-tagged reload after restart.

The next cycle should move this same restart workflow into the metaserver scheduler harness so a
metaserver-issued reload survives node restart without manually invoking lifecycle snapshot routes.

## Cycle 11 Implemented

Cycle 11 moves lifecycle restart recovery into the metaserver scheduler harness:

- The stateful lifecycle nodeserver test double now supports `SaveLifecycleSnapshot` and
  `LoadLifecycleSnapshot` using the same data-node snapshot envelope.
- The scheduler workflow now proves a metaserver-issued load can be snapshotted, restored into a
  restarted node, and followed by a metaserver-issued reload.
- The reload after restore validates scheduler task id, scheduler generation, readonly state, load
  version, and lifecycle report convergence through the scheduler execution path.
- The scenario covers five parity checks: scheduler-issued load, node snapshot save, restarted-node
  snapshot load, scheduler-issued reload after restart, and call ordering through the C++-style
  ServerService lifecycle surface.

The next cycle should persist this lifecycle snapshot automatically during real data-node
load/reload/unload transitions rather than requiring an explicit test/admin save call.

## Cycle 12 Implemented

Cycle 12 makes data-node lifecycle snapshots part of the runtime storage lifecycle:

- `DataNodeRuntime` can now bind to a lifecycle snapshot path, including
  `TS_DATA_NODE_LIFECYCLE_SNAPSHOT` for process startup.
- Runtime construction restores an existing snapshot before worker startup.
- Scheduler lifecycle token installation and lifecycle state transitions automatically persist the
  snapshot, covering load, reload, unload, failed, and transitional states.
- Regression coverage validates five storage checks: token-only persistence, load persistence,
  constructor restore, reload persistence, and unload persistence with scheduler metadata.

The next cycle should expose lifecycle snapshot persistence status in data-node preflight/admin
reports and add startup diagnostics for unreadable or stale lifecycle snapshot files.

## Cycle 13 Implemented

Cycle 13 makes lifecycle snapshot persistence observable for storage operations:

- Data-node preflight now includes lifecycle snapshot persistence status, configured path, last
  restore/persist status, timestamps, and success/failure counters.
- Startup restore failures for unreadable or invalid lifecycle snapshot files are surfaced as
  degraded preflight reasons instead of disappearing silently.
- Server admin routes expose `GetLifecyclePersistence` through the C++-style `ServerService`
  surface and `/server/lifecycle/persistence` through REST.
- Prometheus metrics now report lifecycle snapshot enablement and restore/persist success/failure
  counters.
- Regression coverage validates five observability checks: enabled path reporting, persist success,
  constructor restore status, bad restore failure status, and preflight degradation.

The next cycle should expand page/index recovery consistency reports for orphan pages, missing
index refs, stale refs, corrupt page bytes, and chosen replay boundary.
