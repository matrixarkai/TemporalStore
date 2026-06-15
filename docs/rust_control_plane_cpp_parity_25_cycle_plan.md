# Rust Client/Proxy/Metaserver/Nodeserver C++ Parity Plan

Goal: close the remaining Rust-native control-plane gaps versus C++ TemporalStore without adding
brpc or Thrift surfaces. Each cycle should plan, implement, test locally, update the gap audit, and
push.

## 25-Cycle Backlog

1. Bridge metaserver scheduler lifecycle tokens to nodeserver admin APIs. Done.
2. Execute scheduler load/unload/reload tasks against remote nodeserver HTTP endpoints.
3. Add metaserver task result reporting from nodeserver lifecycle responses.
4. Add scheduler-driven finish-load validation using task id and generation.
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
