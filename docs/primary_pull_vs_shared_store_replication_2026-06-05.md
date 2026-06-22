# Primary-Pull vs Shared-Store Replication Test

Date: 2026-06-05

Cluster:

- AWS region: `us-west-2`
- Metaserver/proxy/client node: `t3.small`, `10.70.1.79`
- Data nodes: `c7i.large`, `10.70.1.163` and `10.70.1.202`
- Extra data node on metaserver instance: `10.70.1.79:17003`
- Storage durability: `storage_async=false`
- Storage path: EFS/shared file backing path
- Replicator settings:
  - `replicator_loop_interval_us=1000`
  - `replicator_max_oplog_per_loop=20000`
  - `replicator_max_indexlog_per_loop=20000`
  - `replicator_update_remote_interval_ms=20`

The test compared two read-only replica recovery/replay paths:

- Primary-pull: `secondary_pull_stream_from_primary=true`
- Shared-store: `secondary_pull_stream_from_primary=false`

Primary-pull was restored after the test. A final smoke test passed:

```text
PASS replication smoke: secondary read matched after 1 attempts, 0 ms
```

## Result Summary

Both modes served primary reads/writes successfully. The primary-pull path had better aggregate-feature replica visibility in this run.

| Test | Primary-pull | Shared-store |
| --- | ---: | ---: |
| STRING write QPS, 20k ops, 2 threads | 195 QPS | 184 QPS |
| STRING write p99 | 19.956 ms | 21.772 ms |
| STRING primary-read QPS | 4,365 QPS | 4,133 QPS |
| STRING primary-read p99 | 2.158 ms | 2.857 ms |
| STRING replica-eligible read QPS | 4,316 QPS | 5,226 QPS |
| STRING replica-eligible read p99 | 2.401 ms | 0.856 ms |
| TemporalAggregate write QPS, 12k writes, 2 threads | 186 QPS | 160 QPS |
| TemporalAggregate write p99 | 21.602 ms | 25.124 ms |
| TemporalAggregate primary-query QPS | 3,597 QPS | 3,663 QPS |
| TemporalAggregate primary-query p99 | 2.419 ms | 2.559 ms |
| TemporalAggregate replica-query QPS | 5,208 QPS | 4,504 QPS |
| TemporalAggregate replica-query p99 | 0.732 ms | 1.533 ms |
| TemporalAggregate replica-query errors | 0 | 19 |
| TemporalAggregate lag sweep visible at t=0 | 1000/1000 | 980/1000 |
| TemporalAggregate lag sweep p99 / max | 0 ms / 0 ms | 347 ms / 639 ms |
| STRING visibility probe p50 / p99 / max | 1.798 ms / 11.944 ms / 17.667 ms | 2.133 ms / 17.888 ms / 18.260 ms |

Result directories on the metaserver node:

- `/var/lib/temporalstore/primary-pull-compare-20260605_101353`
- `/var/lib/temporalstore/shared-store-compare-20260605_102543`

## Interpretation

For this EFS-backed test, primary-pull had the cleaner TemporalAggregate behavior:

- zero aggregate secondary query errors
- immediate aggregate lag-sweep visibility
- lower aggregate write and replica-query p99

Shared-store had one good result: plain STRING replica-eligible reads were faster in the read phase. But the aggregate result matters more for TemporalStore's target use case, because high-cardinality temporal features are the workload where replica freshness and replay correctness are most visible.

The write-side numbers are dominated by durable shared-storage commits. With `storage_async=false`, writes wait for durable log persistence. That protects recovery data but keeps write QPS low on this small EFS-backed cluster.

## Pulling From The New Leader

The current primary-pull code path already follows the metaserver membership view:

- Each partition keeps `primary_partition_id_`.
- `Partition::UpdateMembership` updates that id when metaserver publishes a new primary.
- `RemotePartitionStream::EnsureChannel` rebuilds the channel when the primary endpoint or primary partition id changes.
- Remote stream reads support all required stream kinds:
  - index stream
  - oplog stream
  - page stream

So if the metaserver promotes a secondary and publishes the new membership, other replicas can pull from the new leader without a new stream abstraction.

## Do Replicas Still Need Old Pages?

Yes.

Oplog alone is not enough after the system has dumped object/page state and advanced the replay base. Recovery needs:

- index metadata that says which objects/slots exist
- page metadata and page streams for dumped historical state
- oplog entries after the dumped checkpoint

Primary-pull can fetch old pages only if the new leader has those page streams available. That is fine when the promoted secondary has already restored the pages, or when all nodes can read a shared durable store. It is not enough if:

- the old primary had unique local-only page files,
- the secondary never copied those pages,
- and the old primary is gone.

For production failover, keep one of these designs:

- Shared durable page/index/oplog storage, such as object store or shared file store.
- Primary-pull plus page-copy/snapshot-copy from primary to secondary before promotion.
- Primary-pull plus a bounded oplog retention policy large enough to rebuild from the last shared checkpoint.

## Failover Caveat

The metaserver has `PROMOTE_SECONDARY` support and can choose an existing secondary as the new primary. The current promotion selection is blunt: it chooses a normal secondary, but the inspected code does not yet show a strict freshness gate that verifies the candidate is fully caught up at the promotion point.

Before treating secondary promotion as strong failover, add or verify:

- candidate secondary must have healthy replicator status
- candidate must be at or beyond the committed primary oplog/index-log checkpoint
- candidate must have the required dumped page streams locally or in shared storage
- promotion should fence the old primary
- clients should refresh membership and route writes to the new primary

## Recommendation

Use primary-pull as the default replica catch-up path for the current EFS test cluster, because it gave better aggregate-feature replica visibility and no aggregate secondary query errors in this run.

Keep the shared-store path as a fallback and recovery mode. It is still useful for object-store/EFS recovery, for old page access, and for cases where the old primary cannot serve stream RPCs.
