# TemporalStore AWS Async Storage / Replica Read Run - 2026-06-07

## Goal

Run TemporalStore with `storage_async=true` on the current AWS test cluster and measure how far write QPS and read/query QPS can go on two 2-vCPU data nodes. Also verify whether reads are served only by the primary or can be served by replicas.

## Cluster

- Region: `us-west-2`
- Metaserver/test node: `i-05f55360d92c43908`, private `10.70.1.161`
- Data node 1: `i-0cfbef56e86551535`, private `10.70.1.214`, `c7i.large`, 2 vCPU
- Data node 2: `i-04c93ad8271e5b64a`, private `10.70.1.24`, `c7i.large`, 2 vCPU
- Data ports: `17001`, `17002`
- Metaserver port: `18000`

## Runtime Mode Tested

Both data nodes were restarted with:

- `--storage_async=true`
- `--replicator_loop_interval_us=1000`
- `--replicator_max_oplog_per_loop=20000`
- `--replicator_update_remote_interval_ms=20`
- `--enable_blockcache=true`
- `--blockcache_dram_capacity=8388608`
- `--blockcache_ssd_capacity=67108864`

For the final attempt, both data nodes also had:

- `--secondary_pull_stream_from_primary=true`

## Read Routing Behavior

The client benchmark has a `pin_primary_reads` switch:

- `pin_primary_reads=1`: reads are pinned to the primary.
- `pin_primary_reads=0`: reads are replica-eligible/eventual-consistency reads.

So the code can route reads to replicas for stale-tolerant workloads. It is not a strong read-after-write path. Strong reads should remain primary/leader reads unless a proper freshness/lease/linearizable-read path is added.

## Raft Snapshot Answer

Data-node replication is not currently a true Raft group in this deployed TemporalStore path. Therefore, when a new data node joins, there is no automatic data-node Raft snapshot install for object/page/index state today.

The metaserver itself uses Raft, but in the current AWS process it was launched with:

`--metaserver_snapshot_trigger_interval_sec=0`

That disables the metaserver snapshot trigger in this run. More importantly, metaserver Raft snapshots are metadata snapshots, not data-partition snapshots.

A future data-node Raft design needs explicit snapshot support:

- snapshot payload: object/page/index state plus last-applied oplog id
- install-snapshot RPC/path for a joining or far-behind replica
- log compaction only after all replicas no longer need older oplog records
- read policy that distinguishes primary linearizable reads from replica stale reads

## What Happened

### Attempt 1: `shared-file://` on EFS

Result: blocked before benchmark.

The current metaserver binary rejected the storage URI:

```text
{"status":{"code":9,"message":"invalid storage pool uri"}}
```

This means the deployed metaserver package does not include the newer shared-file URI validation even though the local source code does.

### Attempt 2: `file://` on EFS

Result: tables were accepted, but partitions could not load.

Data-node logs showed:

```text
Flock file failed ... Error:Bad file descriptor
Condition failed ... StoreInternal: Bad file descriptor
Setup condition failed ... StoreInternal: Bad file descriptor
```

So EFS mounted as a normal `file://` local-file store is not a valid shared-store mode for this build. It routes through local-file locking/condition logic and fails on EFS.

### Attempt 3: runtime metaserver

Result: blocked by packaging/runtime mismatch.

Trying to run `/opt/temporalstore/runtime/bin/bcache2-metaserver` failed with:

```text
undefined symbol: bwrite_conv
```

The previous `/opt/temporalstore/bin/bcache2-metaserver` could be restored only with the thrift compatibility shim:

```text
LD_PRELOAD=/opt/temporalstore/runtime/lib/libthrift_conv_shim.so
LD_LIBRARY_PATH=/opt/temporalstore/runtime/lib
```

This confirms the AWS package is currently mixed: server/client runtime artifacts and metaserver artifacts are not from one clean package set.

### Attempt 4: local file + primary-pull mode

Result: benchmark executed, but results are invalid because every operation was reported as an error.

The final run directory on the metaserver node:

```text
/var/lib/temporalstore/async-primary-pull-aggregate-20260607_032748
```

The benchmark produced timings, but `errors == ops` for every phase. Therefore these are failure-path throughput numbers, not successful service QPS.

| Mode | Threads | Phase | Reported QPS | P50 us | P95 us | P99 us | Errors | Valid? |
| --- | ---: | --- | ---: | ---: | ---: | ---: | ---: | --- |
| primary | 1 | TemporalAggregate INCR | 1,954 | 384 | 1,235 | 2,442 | 12,000 / 12,000 | No |
| primary | 1 | TemporalAggregate QUERY | 1,508 | 385 | 2,070 | 3,961 | 1,000 / 1,000 | No |
| primary | 2 | TemporalAggregate INCR | 3,675 | 429 | 1,127 | 2,624 | 12,000 / 12,000 | No |
| primary | 2 | TemporalAggregate QUERY | 3,802 | 406 | 1,179 | 2,502 | 1,000 / 1,000 | No |
| primary | 4 | TemporalAggregate INCR | 5,758 | 505 | 1,974 | 3,252 | 12,000 / 12,000 | No |
| primary | 4 | TemporalAggregate QUERY | 8,064 | 458 | 720 | 990 | 1,000 / 1,000 | No |
| replica-eligible | 1 | TemporalAggregate INCR | 1,762 | 389 | 1,758 | 3,177 | 12,000 / 12,000 | No |
| replica-eligible | 1 | TemporalAggregate QUERY | 1,344 | 406 | 2,323 | 3,649 | 1,000 / 1,000 | No |
| replica-eligible | 2 | TemporalAggregate INCR | 3,360 | 436 | 1,726 | 3,169 | 12,000 / 12,000 | No |
| replica-eligible | 2 | TemporalAggregate QUERY | 3,311 | 419 | 1,869 | 3,241 | 1,000 / 1,000 | No |
| replica-eligible | 4 | TemporalAggregate INCR | 4,182 | 558 | 2,849 | 7,953 | 12,000 / 12,000 | No |
| replica-eligible | 4 | TemporalAggregate QUERY | 5,181 | 547 | 2,035 | 3,488 | 1,000 / 1,000 | No |

Replica lag probes did not reach visibility within the 30s probe window:

```text
replica_lag_probe,0,2687,30004
replica_lag_probe,0,2672,30010
replica_lag_probe,0,2655,30000
```

### Root cause found after log inspection

The data-node logs show the primary failure clearly:

```text
Cmd not found, ModuleId:17, FunctionId:1
```

`ModuleId:17` is `TEMPORAL_AGGREGATE`, and function id `1` is `QUERY`. The same package also failed all TemporalAggregate writes, so the deployed data-node server did not have the TemporalAggregate module registered/linked. The benchmark client can construct TemporalAggregate requests, but the server rejects them before executing the model.

This means the half-hour test did not fail because the aggregate model was too slow. It failed because the deployed server binary/package was not the same as the current source tree.

Local source has the module:

- `src/extension/modules.proto`: `TEMPORAL_AGGREGATE = 17`
- `src/extension/temporal_aggregate/implement.cc`: registers `INCR` and `QUERY`
- `src/extension/temporal_aggregate/CMakeLists.txt`: adds `temporal_aggregate_module`
- `src/CMakeLists.txt`: links module libraries into server/proxy/client targets with `--whole-archive`

The deployed benchmark artifact also looked older than local source: the deployed `temporal_aggregate_scale_benchmark` usage did not include the newer read-mode argument. That is another sign that AWS had a mixed/old package set.

There is a second, separate replica issue:

- `shared-file://` was rejected by the deployed metaserver.
- `file://` over EFS hit local-file locking/condition failures.
- local primary-pull secondary recovery hit missing condition info because condition metadata exists only on the primary local path.

So there were two fixes, not one:

1. Rebuild and redeploy one coherent package so data nodes actually register `TEMPORAL_AGGREGATE`.
2. Fix the replica storage path separately: either make `shared-file://` accepted end to end, or implement primary-pull condition/page bootstrap, or move to the planned Raft snapshot/log path.

Current follow-up status:

- A coherent local `release-coherent` package was rebuilt from one native WSL source tree.
- `bcache2-server` was verified with `strings` to contain `TEMPORAL_AGGREGATE`, `temporal_aggregate::IncrRequest`, `temporal_aggregate::QueryRequest`, and the command registration symbols for INCR/QUERY.
- The matching `temporal_aggregate_scale_benchmark` was verified to contain the newer `read_mode=primary|default|secondary` argument.
- Local package artifacts are:
  - `/tmp/temporalstore-runtime-release-coherent.tar.gz`
  - `/tmp/temporalstore-client-tools-release.tar.gz`
- AWS redeploy and rerun are still pending because the `temporalstore` AWS SSO token expired before upload/deploy.

Before another scale run, run a tiny smoke test and require `errors=0`. Any row with `errors == ops` must be treated as failure-path throughput, not service QPS.

## Valid Historical Async Result To Compare

The earlier clean async write-only comparison in `docs/aws_storage_async_compare_2026-06-06.md` remains the best valid async result until the package/runtime issues are fixed.

For TemporalAggregate INCR with `storage_async=true`:

| Threads | Avg write QPS | Median p99 |
| ---: | ---: | ---: |
| 1 | 1,203/s | 3.068 ms |
| 2 | 2,096/s | 4.397 ms |
| 4 | 3,152/s | 4.837 ms |

That run was write-phase focused, not a fully successful mixed read/write plus replica-read validation.

## Conclusion

No trustworthy new successful async mixed read/write QPS number was produced in this run.

The immediate blockers are AWS redeploy, smoke validation, and storage-mode consistency:

1. Refresh AWS SSO for profile `temporalstore`.
2. Upload and deploy the coherent runtime/client artifacts to the existing AWS cluster.
3. Run a tiny TemporalAggregate INCR/QUERY smoke first and require `errors=0`.
4. Ensure metaserver accepts `shared-file://` and data nodes use the shared-file store implementation for EFS.
5. Keep `file://` for local filesystem only; do not use it for EFS shared-store tests.
6. Re-run with a fresh metaserver state or clean test namespace after fixing the package.
7. Treat replica reads as eventual/stale-tolerant until a freshness guard or Raft/lease read path is implemented.
