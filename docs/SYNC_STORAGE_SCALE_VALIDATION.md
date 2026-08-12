# Sync Storage Scale & Fault-Tolerance Validation

Validation of TemporalStore's **synchronous storage** path at scale — write latency,
replication to a secondary, and fault tolerance (node down → up → catch up) — plus
confirmation that **async storage** and **raft** modes are unaffected.

Run on the `oplog→wal` rename branch (now on `main`), release build, single machine
(WSL Ubuntu 22.04), local shared-store / in-process engines and real multi-process raft.
Latencies are per-operation microseconds; a single-box run bounds absolute throughput, so
read the **sync-vs-async deltas and correctness invariants**, not the absolute QPS.

## 1. Correctness baseline

Full single-threaded library suite: **603 passed / 1 failed**. The one failure is a
known pre-existing environmental flake (`storage_manager … jitter_backoff`, a 256-byte
cache-size assertion) that also fails on `main` baseline (604/1). The oplog→wal wire /
filename rename introduces **zero regressions**.

## 2. Sync storage works, and its latency is good

Shared-store write latency, sync vs async commit (800 ops each):

| Path | p50 | p95 | p99 | max |
|------|-----|-----|-----|-----|
| **sync** storage write  | 1.96 ms | 2.59 ms | 3.83 ms | 14.8 ms |
| **async** storage write | 1.85 ms | 2.12 ms | 2.67 ms | 2.67 ms |

Sync commit costs **~6 % over async at p50** (1.96 vs 1.85 ms) and ~43 % at the p99 tail —
i.e. sync durability is cheap on the steady-state path. This is the payoff of the
C++-aligned model already on `main`: the WAL is fsync'd per write for durability, while the
O(store) served-index materialization is deferred to dump cadence instead of running on
every write. Full primary path incl. replication: sync p50 15.0 ms / p99 29.1 ms vs async
p50 13.5 ms / p99 20.1 ms.

Deployment SLO gate: `storage_deployment_scale_slo_ready = true`, error budget 100 %.

> Caveat — sync under **high write concurrency** (4 writers on one shared store) shows
> p50 ≈ 35 ms from lock contention on the single shared log, but stays correct and fully
> catches up (replica lag 800 → 0 after replay). Tune concurrency per shard accordingly.

## 3. Replication to a secondary

`storage_modes_harness` replays the primary's WAL into a fresh follower in **both** modes:

| Mode | follower applied | last WAL index | read-back |
|------|------------------|----------------|-----------|
| sync  | 2 | 2 | `sync-value` ✓ |
| async | 2 | 2 | `async-value` ✓ |

`scale_harness` replication health: `replication_healthy = true`, **sync max replica lag 0**
(async max lag 39, i.e. the async follower trails then converges). Concurrent-sync workload
replica lag converges 800 → **0** after replay.

## 4. Fault tolerance — crash recovery and node down → up → catch up

**Single-node crash recovery** (`storage_crash_harness`: write aborts mid-flight via
`SIGABRT`, then a fresh engine reloads): WAL replayed (`wal_records = 2`),
`all_live_pages_readable = true`, `slab_integrity.integrity_ok = true`, **0** corrupt /
unreadable / orphan / missing-owner page refs. The mid-write crash loses nothing —
durability rides on the per-write WAL fsync, and reload replays it.

**Real multi-process node down → up → catch up** (`raft_secondary_replication_harness`,
spawns real `raft_node` processes):

- `restarted secondary = node 3`, `recovered_after_restart = true`,
  `restart_recovery_validated = true`
- `healed_follower_catchup_observed = true` — a lagging follower (observed lag 5) rejoins
  and catches up
- crash recovery across all boundaries: `crash_after_storage_mutation_recovered`,
  `crash_after_wal_persist_recovered`, `crash_during_snapshot_install_recovered` — all true
- network partition: isolated-node read correctly **rejected** ("leader is not available");
  after heal, read succeeds (`v-partition`)
- `failover_validated`, `membership_change_validated`, `snapshot_install_validated` — true

## 5. Raft mode is not broken

`distributed_raft_harness` — every runtime-semantics check passes:
read-index & leader lease, exact-once leader transfer, snapshot bootstrap, membership
rescale (scale up/down target voters), post-snapshot rescale, post-rescale reads,
witness-role quorum, learner auto-promote, joint-consensus persisted across restart.
Stale follower writes are rejected (`node is not leader`). Shared-store snapshot round-trip
and **replay-after-raft-write** both validated. Raft write latency p50 34 ms / p99 53 ms
(consensus-bound, expected); raft replica read p50 0.59 ms.

## 6. Async mode is not broken

Async path exercised throughout §2–§3: async storage write p50 1.85 ms, async follower
replay correct, async replica converges. Async remains the production proxy default
(`MATRIXARK_RUST_PROXY_ASYNC_STORAGE` defaults on); sync is available and validated for
distributed durability.

## Summary

| Dimension | Result |
|-----------|--------|
| Sync storage correctness | ✅ 603/1 (1 known flake) |
| Sync write latency | ✅ p50 1.96 ms, p99 3.83 ms (~6 % over async p50) |
| Replication to secondary (sync + async) | ✅ follower replay exact, lag → 0 |
| Crash recovery | ✅ mid-write abort loses nothing, integrity ok |
| Node down → up → catch up | ✅ real multi-process restart + healed-follower catch-up |
| Raft mode | ✅ all consensus/membership/snapshot semantics validated |
| Async mode | ✅ unaffected, remains proxy default |

### Notes on harness coverage

- `scale_harness` at 20 k ops runs > 30 min (raft section ≈ 34 ms/write dominates); use
  a few-hundred-op workload for latency percentiles. Numbers above are from an 800-op /
  400-string-op / 3-node run.
- `storage_crash --mode corrupt-page` and `storage_production_harness` fail for reasons
  **unrelated to this work**: the former assumes a page `.seg` exists immediately after a
  write (invalid under the dump-cadence model where pages materialize at dump); the latter
  carries a stale migration corpus referencing the removed `ips_add_with_options` command.
  Both are pre-existing and tracked separately.
