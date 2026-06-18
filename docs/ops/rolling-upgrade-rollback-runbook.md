# TemporalStore Rust Rolling Upgrade And Rollback Runbook

This runbook covers the open-source Rust deployment shape: metaserver, data nodes, proxy, clients,
and optional local Raft/replica harnesses. It is not a claim of AWS multi-node performance parity or
legacy C++ wire compatibility.

## Preflight Gate

Run these before touching a production-like environment:

```bash
cargo fmt -p temporalstore-rust --check
cargo check -p temporalstore-rust --all-targets
cargo test -p temporalstore-rust --lib production_readiness -- --test-threads=1
cargo run -p temporalstore-rust --bin readiness_gate
cargo run -p temporalstore-rust --bin external_chaos_gate -- --profile quick
```

The readiness gate may still report non-upgrade blockers. For an upgrade window, every service
being upgraded must have:

- no local lifecycle snapshot errors
- no dirty slot dump/install failures
- no stale proxy service-discovery heartbeat
- no metaserver scheduler task stuck in retry without an operator owner
- no ingestion dead-letter spike above the accepted change budget

## Upgrade Order

1. Metaserver followers or standby instances.
2. Data-node secondaries or readonly replicas.
3. Proxies, one at a time.
4. Clients or SDK consumers.
5. Metaserver leader or singleton only after data/proxy convergence is confirmed.
6. Data-node primaries, one shard group at a time.

For each instance:

1. Set the service to drain or readonly when supported.
2. Wait for in-flight worker queues to reach zero or the configured drain timeout.
3. Stop the instance.
4. Start the new binary with the same data and lifecycle directories.
5. Confirm health, readiness, service-discovery heartbeat, and metrics.
6. Confirm route/topology convergence from proxy and client preflight.

## Data-Node Checks

Before stopping a data node:

- `partition_info` reports the expected shard set.
- lifecycle state is not `loading`, `reloading`, or `unloading`.
- storage recovery report has zero corrupt live refs.
- dirty dump/install status has no partial manifest install.
- replica replay lag is within the shard's read-serving policy.

After restart:

- lifecycle snapshot restores the same shard ids and load generations.
- read/write admission policy matches metaserver topology.
- hot cache may be cold, but memory miss -> page/disk read -> refill succeeds.
- Prometheus reports cache, page-store, oplog, ingestion, and lifecycle counters.

## Proxy And Client Checks

Before rotating a proxy:

- `/proxy/preflight` has no stale topology cache reason.
- service-discovery heartbeat is registered and not stale.
- route cache can be refreshed from metaserver.
- drop percent and serving mode match metaserver policy.

After restart:

- `/ProxyService/GetInfo`, `/ProxyService/GetPolicy`, and `/ProxyService/ClientPreflight` respond.
- table-routed command aliases (`Get`, `Set`, `HMGet`, `HMSet`, `HGetAll`, `HLen`) use the normal
  routed client path.
- clients with cached routes survive a temporary metaserver outage for reads allowed by policy.

## Rollback Trigger

Rollback immediately when any of these happen:

- metaserver publishes an unsafe topology transition
- data-node lifecycle snapshot is rejected as stale or incompatible
- proxy preflight reports stale topology after forced refresh
- storage recovery reports corrupt live page bytes or missing index refs
- ingestion duplicate/dead-letter rate exceeds the deployment error budget
- Raft or shared-store replay cannot catch up a follower before the drain timeout

## Rollback Procedure

1. Freeze topology changes in metaserver or stop the scheduler task queue.
2. Stop upgrading new instances.
3. Roll back proxies first if client errors are route/admission related.
4. Roll back data nodes one shard group at a time if errors are storage/lifecycle related.
5. Roll back metaserver last unless topology mutation is the root cause.
6. Re-run preflight and the quick chaos gate.
7. Preserve lifecycle snapshots, slot dump manifests, index logs, oplogs, and proxy/client
   preflight JSON for audit.

## Post-Window Evidence

Attach these artifacts to the upgrade record:

- readiness gate JSON before and after
- proxy and client preflight JSON
- metaserver scheduler task snapshot
- data-node lifecycle and storage recovery reports
- Prometheus scrape around the window
- external chaos gate quick profile output
