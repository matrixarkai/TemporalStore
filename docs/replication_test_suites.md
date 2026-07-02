# TemporalStore Replication Test Suites

This document defines the local guardrails that must pass before AWS replication or scale testing.

## Shared-Store Path

Purpose: verify the existing shared stream/object-store replay path still works for secondary reads.

Local suite:

```bash
BUILD_DIR=<repo>/build \
OUT_DIR=<repo>/output \
bash tools/replication_guardrails_ubuntu22.sh
```

What it does:

- Builds `bcache2-server`, `bcache2-metaserver`, `replication_smoke_example`, and `data_raft_replication_codec_smoke`.
- Starts one metaserver and two data nodes.
- Creates a one-table, two-replica table with `storage_pool_uri=file://...`.
- Forces `--data_replication_mode=shared_store`.
- Forces `--secondary_pull_stream_from_primary=false`, so secondaries recover from the shared store path.
- Runs repeated primary-write to secondary-read visibility checks.
- Fails if server logs show `Partition out of sync` or `replicator out of sync`.

Expected result:

- Each secondary visibility check prints `PASS replication smoke`.
- No out-of-sync errors appear in the data-node logs.

## RustRaft Replication Path

Purpose: guard the new third replication option without regressing existing paths.

Current implemented coverage:

- Server exposes `--data_replication_mode=raft_consensus`.
- `data_raft_replication_codec_smoke` validates committed-log serialization, parse, and corrupt-payload rejection.
- Server exposes `--data_raft_enable_experimental_direct_writes`, and the default is fail-closed for writes.

Current limitation:

- This is not yet a complete data-node Raft group test. The remaining pieces are command proposal before local mutation, full multi-node membership tests, snapshot install validation, leader routing, and read-consistency modes. Those should be added as separate tests as the implementation hardens.

## Required Future Raft Tests

Add these before calling data-node Raft production-ready:

- Three local data-node Raft replicas can form one partition group.
- Writes proposed on the leader are committed and applied on all replicas.
- A follower restart installs snapshot or replays logs and catches up without shared-store reads.
- Leader kill causes a follower to become leader.
- A new follower pulls snapshot plus logs from the Raft group and serves reads after catch-up.
- Linearizable read mode proves a read observes the latest committed write.
- Stale/follower-read mode exposes replication lag metrics and never claims strong consistency.

## AWS Gate

Do not run AWS replication/scale tests until the local suite passes. After local pass, run the same workload on AWS with:

- EFS/shared file store path for shared-store mode.
- Local disk plus `raft_consensus` path once multi-node RustRaft replication is production-ready.
- Concurrent writes and reads.
- Secondary visibility lag measurement with no artificial sleep in the measured path.
