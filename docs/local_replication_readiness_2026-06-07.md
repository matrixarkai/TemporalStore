# Local Replication Readiness - 2026-06-07

## Decision

Do not run AWS performance comparisons for `raft_consensus` as a production replication mode yet.

The current Byteraft-backed data-node path is intentionally fail-closed for writes:

```text
--data_replication_mode=raft_consensus
--data_raft_enable_experimental_direct_writes=false
```

This is correct for safety because the existing command path still mutates local object/oplog/page/index state before a Raft quorum commit. Production Raft requires:

```text
client write
-> serialize deterministic command or mutation
-> Byteraft propose
-> quorum commit
-> FSM apply on leader and replicas
-> local object/oplog/page/index mutation
-> client ack
```

## Local Checks Run

Command:

```bash
cd /root/src/temporalstore
RESULT_DIR=/tmp/temporalstore-readiness-20260607T152830Z \
FORCE_BUILD=0 \
BUILD_JOBS=2 \
bash tools/replication_guardrails_ubuntu22.sh
```

Results:

| Check | Result | Notes |
| --- | --- | --- |
| Data Raft codec smoke | Pass | `SerializeParseRoundTrip` and `RejectsCorruptPayload` passed. |
| `raft_consensus` server flag check | Pass | Server exposes `raft_consensus` and the fail-closed direct-write override flag. |
| Shared-store local two-replica harness | Blocked | The harness started metaserver and two servers and `AddServer` returned OK, but `QueryService/ListServer` stayed `{}` until timeout. This is a local metaserver/server visibility harness issue, not a successful replication result. |

## What This Means

`raft_consensus` should not be benchmarked on AWS for write QPS, read QPS, replica read lag, or failover yet because the production write path is not complete. A benchmark would either fail closed, or require the experimental direct-write flag, which would not represent no-data-loss Raft.

The right AWS comparison matrix after production Raft is complete:

| Mode | Write durability | Reads to test | Metrics |
| --- | --- | --- | --- |
| `raft_consensus` | Ack after Byteraft quorum commit and FSM apply | Leader read, linearizable/read-index, stale replica read, min-index replica read | write QPS, read QPS, p50/p95/p99, CPU, network, Raft commit lag, applied lag, failover RTO/RPO |
| `shared_store --storage_async=false` | Ack after shared-store commit | Primary and replica reads | write/read QPS, EFS latency, replica lag, recovery time |
| `shared_store --storage_async=true` | Ack may precede durable shared-store flush | Primary and replica reads | write/read QPS, async flush lag, possible RPO window |

## Next Engineering Gates

1. Add command serialization before local mutation.
2. Propose writes through `DataRaftConsensusBackend::Propose`.
3. Apply committed entries only from the Byteraft FSM.
4. Implement real partition snapshots: index metadata, live page data, oplog checkpoint, applied Raft index.
5. Add read policies so all replicas can serve reads safely:
   - leader read
   - linearizable/read-index read
   - stale replica read with lag metadata
   - replica-min-index read
6. Build local three-node tests before AWS:
   - normal write/read
   - all replicas serving stale reads
   - measured replica lag with no sleep in the measured path
   - leader kill and follower election
   - new node snapshot install and catch-up

## Current Safe Modes

For data that cannot be lost today, use:

```text
--data_replication_mode=shared_store
--storage_async=false
```

For streaming/replayable data where a small RPO window is acceptable, use:

```text
--data_replication_mode=shared_store
--storage_async=true
```

Use `raft_consensus` for backend bring-up only until the production write and snapshot path is complete.
