# Replication Guardrail Results - 2026-06-07

Local WSL Ubuntu guardrail run:

```bash
bash /root/src/temporalstore/tools/replication_guardrails_ubuntu22.sh
```

Result directory:

```text
/tmp/temporalstore-replication-guardrails-20260607T063329Z
```

## Build/Runtime Inputs

- Server binary: `/root/src/temporalstore/output/bcache2-server`
- Metaserver binary: `/root/src/temporalstore/release-bin-20260606/output/bcache2-metaserver`
- Replication client: `/root/src/temporalstore/build/src/client/example/replication_smoke_example`
- Raft codec smoke: `/root/src/temporalstore/build/src/partition/storage/test/data_raft_replication_codec_smoke`
- Runtime library path included `/root/src/temporalstore/build/lib` for `libthriftd.so.0.11.0`.

## RustRaft Path Guardrails

Passed:

- Server exposes `--data_replication_mode`.
- `data_raft_replication_codec_smoke` passed 2/2 tests:
  - serialize/parse round trip
  - corrupt payload rejection
- Single-replica local cluster started with the guarded Raft path that later became `--data_replication_mode=raft_consensus`.

Important limitation:

This is a readiness guardrail for the new third option, not a full multi-node data-Raft test yet. A complete Raft replication suite still needs command proposal before local mutation, snapshot install validation, leader election, follower catch-up, and read-consistency tests.

## Shared-Store Path Guardrails

Configuration:

- One metaserver
- Two data nodes
- One table
- Two replicas
- `storage_pool_uri=file://.../shared-store/`
- `--data_replication_mode=shared_store`
- `--secondary_pull_stream_from_primary=false`

Secondary visibility checks:

```text
PASS replication smoke: secondary read matched after 2 attempts, 112 ms
PASS replication smoke: secondary read matched after 1 attempts, 14 ms
PASS replication smoke: secondary read matched after 1 attempts, 13 ms
PASS replication smoke: secondary read matched after 1 attempts, 15 ms
PASS replication smoke: secondary read matched after 2 attempts, 109 ms
```

Out-of-sync scan:

- No `Partition out of sync`
- No `replicator out of sync`

Final result:

```text
PASS replication guardrails
```
