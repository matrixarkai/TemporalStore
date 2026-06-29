# Storage And Raft Production Readiness Plan

This page is the detailed storage/Raft evidence plan. The consolidated Rust-vs-C++ parity status
across all subsystems is tracked in
[`rust_vs_cpp_temporalstore_parity_report.md`](rust_vs_cpp_temporalstore_parity_report.md).

## Summary

This page turns the storage/Raft production-readiness order into an executable local gate:

```bash
tools/run_storage_raft_production_readiness.sh
```

The gate runs the current Rust local harnesses one by one, validates their JSON output, runs the
feature-gated TemporalRaft adapter tests, and then prints the remaining readiness blockers. Production
distributed Raft mode is mandatory: local Raft is test-only and cannot be selected as a runtime
deployment mode. This gate does not claim full production readiness while the readiness gate still
reports missing durable real-process TemporalRaft rollout, atomic snapshot/storage persistence, and
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
   - Runs `cargo test -p temporalstore-rust --features temporal-raft-engine temporal_raft_ --lib`.
   - Runs `readiness_gate --service raft_replication`.
   - Requires TemporalRaft-backed data-node/metaserver adapter evidence and production process startup
     defaults, then reports the remaining blockers around durable real-process rollout, snapshot
     atomicity, mTLS, and external chaos.

5. **Raft snapshot/restart/failover harness**
   - Runs `distributed_raft_harness`.
- Runs `metaserver_raft_harness`.
- Runs `raft_secondary_replication_harness`.
- Builds and validates `raft-distributed-parity.json` from those three harness outputs.
- Builds `cpp-raft-cases-on-rust.json` from the unified C++ Raft corpus so the final proof compares
  Rust harness evidence against the C++ scenario names for leader election, failover, snapshot
  install/restart, membership add/promote/remove, follower lag, and secondary reads.
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

- Feature-gated TemporalRaft data-node and metaserver adapter.
- TemporalRaft boundary types for entries, log ids, membership, and snapshot metadata.
- Durable TemporalRaft adapter state with log records, state-machine apply, snapshot build/install
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
- Dedicated `validate_rustraft_derived_readiness.py` static gate for RustRaft-derived readiness:
  election/pre-vote guards, durable hard state and membership, safe joint-consensus scale changes,
  leader lease/read-index behavior, bounded stale reads, learner promotion, leader transfer,
  snapshot bootstrap, lag/catch-up, failover, operator status/local-status/metrics, and operator
  routes, plus RPC retry/backpressure/auth/deadline behavior, bounded WAL retention, and
  applied-log-byte snapshot triggers.
- WAL-backed node records now carry a durable apply/snapshot fence for commit index, applied index,
  installed snapshot floor, and first retained log index, giving the TemporalRaft path a concrete
  RustRaft-style applied-index/storage/snapshot atomicity contract to preserve.
- WAL-backed node records now also carry `RaftStorageApplyFence` for shard id, Raft term,
  committed/applied index, snapshot id, storage epoch, and checksum, so recovery rejects missing,
  corrupt, stale, or ahead-of-storage fence state before replay.
- TemporalRaft durable state now carries the same `RaftStorageApplyFence`, refreshes it after
  synchronous engine apply and snapshot creation, persists the state through temp-file fsync plus
  rename, and rejects corrupt fences on restart. This closes the local applied-index/storage/snapshot
  atomicity contract; real multi-process data-node rollout validation remains the production
  blocker.
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

The readiness gate now fails closed for the Raft replication and data-node distributed Raft slices
unless it is given explicit multi-process TemporalRaft rollout evidence for both data-node and
metaserver paths. Local in-process Raft fixtures and local harnesses remain useful supporting
evidence, but they cannot satisfy production readiness by themselves:

- Raft TemporalRaft rollout readiness is now explicit in the readiness gate: adapter presence,
  data-node/metaserver startup selection, durable local log state, data-node process API writes,
  data-node restart/snapshot/applied-fence validation, failover, membership-change, follower-lag,
  and secondary-read validation, metaserver process API mutations, metaserver
  read-index/snapshot/scheduler replay validation, metaserver failover/membership/lag/secondary-read
  validation, and multi-process log-store validation are required.
- Raft atomic apply readiness is now explicit in the readiness gate: storage apply fence
  persistence, WAL fence recovery validation, production runtime data-node atomic durability
  reports, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle
  reporting are covered.
- Raft metaserver membership readiness is now explicit in the readiness gate: topology membership
  plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, voter removal,
  networked scheduler `/raft/membership/apply` transport, persisted scheduler task state, and
  metaserver-owned data-node Raft execution under follower lag, failover, scale up/down, and
  secondary replication are covered.
- Raft transport security readiness is now explicit in the readiness gate: auth-token validation,
  mTLS cert/key/CA config validation, service-process mTLS runtime selection, authenticated HTTP
  transport, and plaintext-only local chaos guardrails are covered. `TS_RAFT_SECURITY_MODE=mtls`
  now selects the mTLS process path and rejects plaintext unless local chaos explicitly allows it.
- Raft external chaos readiness is now explicit in the readiness gate: local OS-process
  restart/failover, stale-read partition heal, lagging follower catch-up, networked
  membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates
  are covered by the local external chaos gate. Cloud-provider packet-loss/disk-pressure injection
  still belongs in the AWS/Docker scale evidence package.
- Rust-local storage migration corpus readiness is now explicit in the readiness gate: converted
  corpus replay through engine restart, Redis/admin reads, shared-store sync/async replay, cache
  warmup, Raft read paths, the external C++ binary-artifact exporter, CI-published golden artifact
  directory, and the unified C++/Rust runner are covered. The compatibility decision remains
  migration-only into Rust-native page/log formats, not byte-for-byte C++ page/log layout.
- Local/shared-store object manifest dependency matrix is now explicit in the readiness gate:
  local file objects, checkpoint manifests, oplog cursor retention, page segment manifests, and
  follower-cursor retention plus Raft snapshot manifest retention are covered for the
  local-file/shared-store target. Live ByteStore/S3 object-store dependency wiring remains blocked
  until those backends are implemented and validated.
- Local cache pressure coverage is now explicit in the readiness gate: memory read-through, disk
  block cache, admission/eviction counters, slot warmup, cache invalidation, SSD tiering policy,
  admission tuning, and long-running pressure validation evidence are covered.
- Storage readiness evidence maps the C++ parity storage internals to concrete fields:
  `storage_index.native_slot_object_page_authority_ready`,
  `storage_index.slotstore_layout_transition_ready`, and
  `storage_index.object_manager_runtime_ready`. Cache pressure soak evidence is tracked separately
  as `storage_cache_mtcache.cache_pressure_soak_restart_ready`, including memory/disk pressure and
  restart refill from the persisted disk cache.
- The remaining storage parity evidence fields are explicit as well:
  `storage_compaction.model_layout_policy_ready`,
  `storage_dump_load.merged_recovery_ready`,
  `storage_manager.phase_loop_ready`,
  `storage_manager.gc_eviction_pressure_ready`, and
  `storage_cache_mtcache.cache_pressure_soak_restart_ready`.
- GC/eviction evidence now exposes typed page-GC retention counters for live refs, slot dump
  manifests, shared-store replay cursors, Raft snapshot refs, checkpoint floors, Raft install
  floors, and delayed-destroy grace. WAL reclaim evidence covers durable slot-generation
  frontiers, follower cursors, and Raft snapshot retain floors before truncation.
- ByteStore/S3 live backend integration tied to follower cursors and Raft snapshots.

The local gate now writes one combined proof envelope:

```text
<artifact-dir>/storage-raft-production-proof.json
```

The proof format is `temporalstore_storage_raft_production_proof_v1` and combines:

- `storage-fault-matrix.json` for corrupt, missing, stale, and partial dump/load faults
- `storage-production.json` for C++ migration corpus replay through restart, Redis/admin,
  shared-store sync/async, cache warmup, and Raft read paths
- `storage-modes.json` for local file/shared-store and local WAL restore evidence
- `raft-distributed-parity.json` for data-node plus metaserver TemporalRaft rollout evidence,
  membership, snapshots, failover, follower lag, and secondary reads
- `cpp-raft-cases-on-rust.json` for C++ Raft scenario comparison against the Rust data-node and
  metaserver harness evidence
- `external-chaos.json` for local packet-loss/disk-pressure/process-chaos slice evidence
- `raft-readiness.json` for remaining global production blockers

`local_production_ready_slice=true` means the local/shared-store storage plus Raft plus cache
pressure proof passed in one artifact set. `global_production_ready` remains false until the
broader deployment evidence is present, including Docker/AWS multi-service SLO evidence and any
explicitly scoped live external object-store integration. Storage compatibility remains
behavioral/migration-based into Rust-native page/log formats, not byte-for-byte C++ layout.

The proof contains two parity-focused subreports:

- `evidence.cpp_raft_scenario_comparison` requires shared C++ Raft cases for election, failover,
  snapshot, membership, follower lag, and secondary reads, then maps each scenario to concrete Rust
  TemporalRaft harness fields. Local Raft fixtures are marked test-only through
  `evidence.local_raft_fixture_policy`; they cannot satisfy production readiness.
- `evidence.unified_storage_recovery_dump_load_cache_gc` ties storage recovery, dump/load,
  cache-pressure/refill, follower-safe GC/shared-store replay, and C++ migration-corpus evidence
  into the same readiness envelope.

## Local Commands

Fast static validation:

```bash
python3 tools/validate_storage_raft_production_plan.py
python3 tools/validate_raft_storage_parity_evidence.py
python3 tools/build_storage_raft_production_proof.py --artifact-dir <artifact-dir>
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

Strict mode is expected to fail until durable real-process TemporalRaft rollout, atomic
snapshot/storage persistence, and external distributed fault blockers are closed. Local-model harness
success is validation evidence only; it does not satisfy the production Raft readiness gate.

Storage keeps local/shared-store correctness separate from broad release evidence. Storage/cache
readiness is strong for Rust-native local/shared-store paths: the local Rust-native storage harness
evidence covers dump/load, cache pressure, restart recovery, shared-store replay, and Raft movement.
The broader Docker/AWS deployment-scale SLO report is tracked separately by
`scale_slo_report.storage_deployment_scale_slo_ready` and covers metaserver, proxy, client,
data-node, Raft failover, storage pressure, cache pressure, proxy convergence, workload replay,
p50/p95/p99, throughput, error budget, CPU/memory/disk/network collectors, replica lag, failover
count, and scale events. Live external object-store evidence, including ByteStore/S3 follower-cursor
and Raft-snapshot manifest retention, is also scoped separately unless that backend is explicitly
enabled for a release target.
