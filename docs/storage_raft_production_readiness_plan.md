# Storage And Raft Production Readiness Plan

## Summary

This page turns the storage/Raft production-readiness order into an executable local gate:

```bash
tools/run_storage_raft_production_readiness.sh
```

The gate runs the current Rust local harnesses one by one, validates their JSON output, runs the
feature-gated OpenRaft adapter tests, and then prints the remaining readiness blockers. Production
distributed Raft mode is mandatory: local Raft is test-only and cannot be selected as a runtime
deployment mode. This gate does not claim full production readiness while the readiness gate still
reports missing durable real-process OpenRaft rollout, atomic snapshot/storage persistence, and
external distributed fault validation.

## Current Execution Order

1. **Storage recovery/fault matrix hardening**
   - Runs `storage_fault_matrix_harness`.
   - Validates checksum mismatch, partial manifest, missing segment, stale manifest, and corrupt
     page segment rejection, plus restart-during-install roll-forward.

2. **Slot dump/load atomicity and manifest rejection**
   - Runs `storage_production_harness`.
   - Validates slot dump manifest creation, restart install, recovery reports, shared-store sync and
     async replay, and Raft leader-transfer reads.

3. **Follower-safe GC and cache pressure**
   - Runs `storage_modes_harness`.
   - Validates sync/async shared-store replay, local file storage, and WAL-backed restore evidence.

4. **Real Raft FSM/storage selection and integration**
   - Runs `validate_storage_raft_production_plan.py`.
   - Runs `cargo test -p temporalstore-rust --features openraft-engine openraft_ --lib`.
   - Runs `readiness_gate --service raft_replication`.
   - Requires OpenRaft-backed data-node/metaserver adapter evidence and production process startup
     defaults, then reports the remaining blockers around durable real-process rollout, snapshot
     atomicity, mTLS, and external chaos.

5. **Raft snapshot/restart/failover harness**
   - Runs `distributed_raft_harness`.
- Runs `metaserver_raft_harness`.
- Runs `raft_secondary_replication_harness`.
- Builds and validates `raft-distributed-parity.json` from those three harness outputs.
- Validates proposal, follower write rejection, leader transfer, data-node and metaserver
     membership change, metaserver snapshot restore plus lagging-voter tail catch-up, external
     snapshot bootstrap/read, secondary restart catch-up, partition heal, follower lag/catch-up,
     stale vote rejection, rolling restart, and leader-crash failover reads.

6. **Combined storage+Raft production harness**
   - Runs `external_chaos_gate --profile quick`.
   - The quick profile composes OS-process Raft restart/failover, networked membership/snapshot,
     storage modes, storage fault matrix, and storage production harness scenarios.

7. **Update unified C++/Rust corpus and readiness docs**
   - Validates `compat/unified_temporalstore_cases.json`.
   - Validates duplicate-test rules.
   - Validates Raft/storage parity evidence gates.
   - Runs `git diff --check`.

## What Is Covered Locally Now

Storage/cache local coverage:

- Slot-scoped dump manifests.
- Manifest checksum and sequence validation.
- Missing/corrupt/stale/partial manifest rejection.
- Restart-during-install marker roll-forward for safe slot dump installs.
- Recovery reports before and after restart.
- Cache warmup during lifecycle apply.
- Shared-store sync and async replay.
- Local storage modes and WAL-backed restore.
- Storage production harness covering dump, cache pressure, restart recovery, shared-store replay,
  and Raft movement.

Raft local coverage:

- Feature-gated OpenRaft data-node and metaserver adapter.
- OpenRaft boundary types for entries, log ids, membership, and snapshot metadata.
- Durable OpenRaft adapter state with log records, state-machine apply, snapshot build/install
  metadata, read-index checks, membership changes, leader transfer, and restart recovery tests.
- Data-node Raft consensus contract fields aligned with C++ first: learner bootstrap,
  learner auto-promotion, fatal/snapshot status fields, campaign/forced campaign control, and
  fail-closed unavailable-backend behavior.
- Metaserver Raft distributed/fault contract aligned with C++ control gates: membership list/add/remove,
  log-applied read-index wait, snapshot trigger, lagging voter catch-up after stale snapshot install,
  leader transfer, failover after leader loss, and explicit fail-closed handling for unsupported
  metaserver learner/witness membership.
- Dedicated `metaserver_raft_harness` JSON gate for local multi-node membership, snapshot restore,
  lagging-voter tail catch-up, failover, and no-majority rejection parity.
- Dedicated `run_raft_distributed_parity.sh` JSON gate that composes data-node distributed Raft,
  data-node secondary/fault tolerance, and metaserver Raft parity in one C++-mapped local run.
- Dedicated `validate_byteraft_derived_readiness.py` static gate for ByteRaft-derived readiness:
  election/pre-vote guards, durable hard state and membership, safe joint-consensus scale changes,
  leader lease/read-index behavior, bounded stale reads, learner promotion, leader transfer,
  snapshot bootstrap, lag/catch-up, failover, operator status/local-status/metrics, and operator
  routes, plus RPC retry/backpressure/auth/deadline behavior, bounded WAL retention, and
  applied-log-byte snapshot triggers.
- WAL-backed node records now carry a durable apply/snapshot fence for commit index, applied index,
  installed snapshot floor, and first retained log index, giving the OpenRaft path a concrete
  ByteRaft-style applied-index/storage/snapshot atomicity contract to preserve.
- WAL-backed node records now also carry `RaftStorageApplyFence` for shard id, Raft term,
  committed/applied index, snapshot id, storage epoch, and checksum, so recovery rejects missing,
  corrupt, stale, or ahead-of-storage fence state before replay.
- Data-node Raft snapshot install now reports freeze, flush, manifest verification, checksum
  verification, install completion, tail replay, and rollback decisions through
  `RaftSnapshotInstallReport`; full production readiness still requires the real process
  freeze/flush/download/install harness.
- `ProductionMetaRaftRuntime` now exposes a metaserver-owned data-Raft membership workflow report
  covering learner add, catch-up verification, promotion, leader transfer, and voter removal; full
  production readiness still requires the networked scheduler harness against real data-node
  processes.
- Data-node Raft log matching is snapshot-floor aware: post-compaction entries continue after the
  installed snapshot index and AppendEntries can match previous terms against either retained log
  entries or the installed snapshot floor.
- Data-node replica catch-up installs the leader snapshot floor before replaying the retained
  post-snapshot tail, so newly added or lagging replicas do not serve tail-only state after
  compaction.
- Data-node AppendEntries apply rejects compacted entries at or below the installed snapshot floor,
  preventing stale log replay over snapshotted state.
- Metaserver Raft status, election freshness, failover, catch-up, and add-node paths now account
  for installed snapshot floors instead of treating compacted voters as log-empty.
- Local production Raft runtime wrapper.
- HTTP Raft transport for proposal/read/admin paths.
- WAL-backed local recovery.
- Leader transfer.
- Membership scale down/up.
- Post-snapshot data-node re-scale down/up with committed write/read convergence.
- Metaserver post-failover voter replacement and follow-up scale-down with committed route reads.
- External snapshot publish/bootstrap/read.
- Secondary restart catch-up.
- Partition stale-read rejection and heal.
- Lagging follower observation and catch-up.
- Rolling restart.
- Leader-crash failover reads.

## Remaining Production Blockers

The readiness gate intentionally still blocks production readiness on:

- Production OpenRaft durable log-store rollout across real data-node process groups.
- Production OpenRaft durable log-store rollout across real metaserver process groups.
- Raft atomic apply readiness is now explicit in the readiness gate: storage apply fence
  persistence, WAL fence recovery validation, and snapshot lifecycle reporting are covered. Real
  storage-mutation and snapshot-install atomic commit integration remains blocked until wired
  through the data-node process path, matching the ByteRaft/ByteKV replica-engine recovery
  contract.
- Raft metaserver membership readiness is now explicit in the readiness gate: topology membership
  plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, and voter removal
  are covered. Networked scheduler transport, persisted scheduler task state, and real data-node
  Raft group execution remain blocked until the metaserver drives `/raft/membership/apply`
  against production data-node groups.
- Raft transport security readiness is now explicit in the readiness gate: auth-token validation,
  mTLS cert/key/CA config validation, authenticated HTTP transport, and plaintext-only local chaos
  guardrails are covered. Real service-process mTLS enforcement remains blocked until every
  production Raft API path requires it at runtime.
- Raft external chaos readiness is now explicit in the readiness gate: local OS-process
  restart/failover, stale-read partition heal, lagging follower catch-up, networked
  membership/snapshot, and storage replay gates are covered. External multi-process packet-loss,
  disk-pressure, and process-chaos tests remain blocked until run against production-like
  deployments.
- Rust-local storage migration corpus readiness is now explicit in the readiness gate: converted
  corpus replay through engine, shared-store, Raft read paths, and the unified C++/Rust runner is
  covered. The external C++ binary-artifact exporter and CI-published golden storage migration
  corpus remain blocked until the C++ build publishes real binary page/log artifacts.
- Local/shared-store object manifest dependency matrix is now explicit in the readiness gate:
  local file objects, checkpoint manifests, oplog cursor retention, page segment manifests, and
  follower-cursor retention are covered for the local-file/shared-store target. Live ByteStore/S3
  object-store dependency wiring remains blocked until those backends are implemented and validated.
- Local cache pressure coverage is now explicit in the readiness gate: memory read-through, disk
  block cache, admission/eviction counters, slot warmup, cache invalidation, and tiny-cache pressure
  harness evidence are covered. Production SSD cache tiering policy, admission tuning, and
  long-running live pressure validation remain blocked until exercised in a production-like run.
- ByteStore/S3 live backend integration tied to follower cursors and Raft snapshots.

## Local Commands

Fast static validation:

```bash
python3 tools/validate_storage_raft_production_plan.py
python3 tools/validate_raft_storage_parity_evidence.py
```

Full local storage/Raft production-readiness gate:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-storage-raft-production-target \
TS_STORAGE_RAFT_TIMEOUT=120s \
tools/run_storage_raft_production_readiness.sh
```

Raft-only data-node plus metaserver C++ parity gate:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-raft-distributed-parity-target \
TS_RAFT_PARITY_TIMEOUT=180s \
tools/run_raft_distributed_parity.sh
```

Strict readiness mode:

```bash
TS_REQUIRE_STORAGE_RAFT_READY=1 tools/run_storage_raft_production_readiness.sh
```

Strict mode is expected to fail until durable real-process OpenRaft rollout, atomic
snapshot/storage persistence, and external distributed fault blockers are closed. Local-model harness
success is validation evidence only; it does not satisfy the production Raft readiness gate.
