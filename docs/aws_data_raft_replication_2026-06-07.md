# AWS Data-Node Raft-Forward Replication Test - 2026-06-07

Status: historical. This run tested the old RPC-forwarded committed-oplog path. That path has
been removed from active data-node Raft code after switching `raft_consensus` to Byteraft.

## Scope

This run tested the old standalone data-node Raft-style replication path on the existing AWS one-cluster deployment. No new AWS cluster was created.

Topology:

- Metaserver/client node: `i-05f55360d92c43908`, `10.70.1.161`, port `17000`
- Data node 1: `i-0cfbef56e86551535`, `10.70.1.214`, port `17001`
- Data node 2: `i-04c93ad8271e5b64a`, port `17002`
- Data-node instances: 2 vCPU class

Runtime:

- Fresh server built in WSL Ubuntu 22.04.
- Runtime installed under `/opt/temporalstore/raft-runtime` and aligned to `/opt/temporalstore/bin` on the data nodes.
- Local codec smoke passed before AWS deployment:
  - `DataRaftReplicationTest.SerializeParseRoundTrip`
  - `DataRaftReplicationTest.RejectsCorruptPayload`

## Historical Path

The removed forwarder path was isolated from the existing shared-store replicator and used a now-removed experimental mode.

The new path:

1. Primary tails its local oplog.
2. Primary serializes each committed `storage::OpLog` as a `DataRaftLogEntry`.
3. Primary forwards that entry to replica data nodes through `ApplyDataRaftLog`.
4. Replica appends the replayed oplog into its own local stream.
5. Replica applies the oplog through `ObjectManager::ReplayOplog` using the local log address.

This avoids using EFS/shared object storage for the normal write replication path.

Important limitation: this was a standalone Raft-style forwarding/apply path, not a full quorum Raft implementation with durable per-shard Raft WAL, ReadIndex, automatic leader election, and snapshot installation. Active data-node Raft work now uses `--data_replication_mode=raft_consensus` with Byteraft instead.

## AWS Raft Result

Data nodes were started with the removed forwarder flags plus async storage and zero delayed dump length. Those flags are intentionally not listed as runnable guidance because the old forwarder code has been deleted.

Storage URI for the table:

```text
file:///var/lib/temporalstore/raft-local/raft-20260607_132042/
```

Secondary visibility smoke:

```text
PASS replication smoke: secondary read matched after 1 attempts, 0 ms
```

STRING benchmark, 3,000 writes then 3,000 reads:

| Threads | Set QPS | Set p50 | Set p95 | Set p99 | Get QPS | Get p50 | Get p95 | Get p99 | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1,623 | 402 us | 2,010 us | 4,110 us | 2,127 | 337 us | 1,328 us | 2,421 us | 0 |
| 2 | 3,325 | 428 us | 1,876 us | 3,070 us | 3,070 | 399 us | 2,108 us | 3,981 us | 0 |

CPU snapshots after the run:

| Node | Mode | CPU | Memory | Notes |
|---|---|---:|---:|---|
| data01 | raft | 12.8% | 2.1% | primary-preferred |
| data02 | raft | 5.3% | 1.9% | secondary |

## Shared-Store Comparison Attempt

Shared-store was tested with:

```text
--data_replication_mode=shared_store
--secondary_pull_stream_from_primary=false
--replicator_loop_interval_us=1000
--replicator_max_oplog_per_loop=20000
--replicator_max_indexlog_per_loop=20000
--replicator_update_remote_interval_ms=20
--storage_async=true
```

The initial `shared-file://` URI was rejected by the current metaserver validator:

```text
invalid storage pool uri
```

The harness was changed to use the EFS-mounted file URI:

```text
file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260607_132447/
```

Data nodes started and loaded partitions, but the client failed to refresh routing:

```text
FAIL open table: Internal: Update router failed
```

Observed CPU snapshots after the failed shared-store attempt:

| Node | Mode | CPU | Memory | Notes |
|---|---|---:|---:|---|
| data01 | shared_store | 15.3% | 2.5% | partitions appeared to load |
| data02 | shared_store | 4.7% | 1.3% | server was listening |

Server logs showed repeated bvar metric duplicate warnings for the shared-store test table. They did not crash the server, but the client could not obtain usable routing for the benchmark. Because of this, there is no clean same-build shared-store latency comparison yet.

## Current Verdict

Raft apply/forwarding path:

- Built successfully.
- Local codec tests passed.
- AWS secondary visibility smoke passed.
- AWS STRING write/read benchmark completed with zero errors.
- Secondary could serve the replicated smoke read immediately in this run.

Shared-store path with the same fresh runtime:

- Data nodes started.
- Table creation succeeded with EFS `file://` URI.
- Client route update failed, so the benchmark did not run.
- This is a no-regression blocker to investigate before claiming shared-store parity on this build.

## Follow-Ups

1. Fix shared-store route refresh failure in the current package or harness.
2. Add a component test that applies forwarded logs to a replica partition and then queries the replica directly.
3. Add real data-node Raft WAL/quorum support instead of only RPC forwarding.
4. Add snapshot install for new or far-behind replicas.
5. Add read modes: leader, linearizable/read-index, replica-stale, and replica-min-index.
