# Storage And Raft Production Readiness Plan

## Summary

This page turns the storage/Raft production-readiness order into an executable local gate:

```bash
tools/run_storage_raft_production_readiness.sh
```

The gate runs the current Rust local harnesses one by one, validates their JSON output, and then
prints the remaining readiness blockers. It does not claim full production readiness while the
readiness gate still reports missing real OpenRaft or raft-rs FSM/storage integration.

## Current Execution Order

1. **Storage recovery/fault matrix hardening**
   - Runs `storage_fault_matrix_harness`.
   - Validates checksum mismatch, partial manifest, missing segment, stale manifest, and corrupt
     page segment rejection.

2. **Slot dump/load atomicity and manifest rejection**
   - Runs `storage_production_harness`.
   - Validates slot dump manifest creation, restart install, recovery reports, shared-store sync and
     async replay, and Raft leader-transfer reads.

3. **Follower-safe GC and cache pressure**
   - Runs `storage_modes_harness`.
   - Validates sync/async shared-store replay, local file storage, and WAL-backed restore evidence.

4. **Real Raft FSM/storage selection and integration**
   - Runs `validate_storage_raft_production_plan.py`.
   - Runs `readiness_gate --service raft_replication`.
   - Reports the current blocker: real OpenRaft or raft-rs data-node FSM/storage implementation is
     still not complete.

5. **Raft snapshot/restart/failover harness**
   - Runs `distributed_raft_harness`.
   - Runs `raft_secondary_replication_harness`.
   - Validates proposal, follower write rejection, leader transfer, membership change, external
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
- Recovery reports before and after restart.
- Cache warmup during lifecycle apply.
- Shared-store sync and async replay.
- Local storage modes and WAL-backed restore.
- Storage production harness covering dump, cache pressure, restart recovery, shared-store replay,
  and Raft movement.

Raft local coverage:

- Local production Raft runtime wrapper.
- HTTP Raft transport for proposal/read/admin paths.
- WAL-backed local recovery.
- Leader transfer.
- Membership scale down/up.
- External snapshot publish/bootstrap/read.
- Secondary restart catch-up.
- Partition stale-read rejection and heal.
- Lagging follower observation and catch-up.
- Rolling restart.
- Leader-crash failover reads.

## Remaining Production Blockers

The readiness gate intentionally still blocks production readiness on:

- Real OpenRaft or raft-rs data-node FSM/storage integration.
- Networked metaserver Raft transport and scheduler loop that automatically drives real data-node
  membership changes.
- External multi-process packet-loss and disk-pressure tests.
- External C++ binary-artifact exporter and CI-published golden storage migration corpus.
- Restart-during-install dump/load fault injection plus live object-store manifest dependency
  matrix.
- Production SSD cache tiering policy, admission tuning, and long-running pressure validation.
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

Strict readiness mode:

```bash
TS_REQUIRE_STORAGE_RAFT_READY=1 tools/run_storage_raft_production_readiness.sh
```

Strict mode is expected to fail until the real OpenRaft or raft-rs FSM/storage blocker is closed.
