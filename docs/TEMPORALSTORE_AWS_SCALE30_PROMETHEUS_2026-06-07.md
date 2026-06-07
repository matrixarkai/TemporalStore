# TemporalStore AWS 30-Minute Scale And Prometheus Snapshot - 2026-06-07

## Cluster

| Role | Instance | Private IP | Public IP | Type |
|---|---|---|---|---|
| meta / client / UI | `i-05f55360d92c43908` | `10.70.1.161` | `35.92.109.186` | `t3.small` |
| data01 | `i-0cfbef56e86551535` | `10.70.1.214` | `34.220.17.155` | `c7i.large` |
| data02 | `i-04c93ad8271e5b64a` | `10.70.1.24` | `34.220.9.104` | `c7i.large` |

## Public URLs

- Website / UI: `http://35.92.109.186:8088/`
- Prometheus text endpoint: `http://35.92.109.186:8088/temporalstore-vars.prom`

The Prometheus exporter was reset to scrape:

```text
metaserver=http://127.0.0.1:18000/vars
data01=http://10.70.1.214:17001/vars
data02=http://10.70.1.24:17002/vars
```

Final public endpoint source counts:

| Source | Series Count |
|---|---:|
| `data01` | 3,119 |
| `data02` | 1,580 |
| `metaserver` | 811 |

## Runtime Setup

- Metaserver: `10.70.1.161:18000`
- Data server: `10.70.1.214:17001`
- Data server: `10.70.1.24:17002`
- Storage URI used for the primary-only scale table: `file:///mnt/temporalstore-shared/aws-prom/storage/`
- Table: `aws30m_primary/temporalagg`
- Run directory on meta node: `/var/lib/temporalstore/scale30_20260607T003847Z`
- SSM command id: `523512b6-2e03-47aa-8bb3-0931982de668`

Note: the two-replica table path was not used for the 30-minute run because secondary readonly load repeatedly hit `Missing condition info` in this deployed runtime. The successful table for the load was primary-only.

## 30-Minute Result

Time window:

```text
start_utc=2026-06-07T00:38:47+00:00
end_utc=2026-06-07T01:08:47+00:00
```

Workload:

```text
module_ingest_query_loop
```

The packaged client loop executed these modules once per iteration before reaching the TemporalAggregate step:

| Module | Pass Count |
|---|---:|
| `STRING` | 1,525 |
| `COMMON` TTL/delete | 1,525 |
| `HASH` | 1,525 |
| `SET` | 1,525 |
| `FEATURE` time sequence | 1,525 |
| `IPS` | 1,525 |
| `RISK` window count | 1,525 |

TemporalAggregate did not pass in this deployed artifact:

```text
1525 FAIL: TEMPORAL_AGGREGATE Incr failed_login_count: Internal: Request server resp size check failed
```

Interpretation:

- The service stayed up for the 30-minute loop.
- The general module smoke path was stable across 1,525 iterations.
- This is not a valid successful TemporalAggregate scale run because every TemporalAggregate call failed with the response-size check issue.
- The next engineering task is to debug TemporalAggregate response encoding / client-server compatibility on the deployed release artifact before rerunning high-cardinality aggregate scale.

## Selected Prometheus Data

From the final multi-source Prometheus snapshot:

```text
bthread_count{source="data01"} 5.0
bthread_count{source="data02"} 5.0
bthread_count{source="metaserver"} 17.0
bthread_worker_usage{source="data01"} 1.00402
bthread_worker_usage{source="data02"} 1.00756
bthread_worker_usage{source="metaserver"} 2.41845
process_cpu_usage{source="data01"} 0.12
process_cpu_usage{source="data02"} 0.027
process_cpu_usage{source="metaserver"} 1.006
process_cpu_usage_user{source="data01"} 0.113
process_cpu_usage_user{source="data02"} 0.022
process_cpu_usage_user{source="metaserver"} 0.921
process_cpu_usage_system{source="data01"} 0.007
process_cpu_usage_system{source="data02"} 0.005
process_cpu_usage_system{source="metaserver"} 0.085
```

Sample storage-stream metrics for the primary table:

```text
partition_blob_append_latency_..._aws30m_primary_..._index_..._count{source="data01"} 15779.0
partition_blob_append_latency_..._aws30m_primary_..._index_..._latency{source="data01"} 2694.0
partition_blob_append_latency_..._aws30m_primary_..._index_..._latency_99{source="data01"} 3149.0
partition_blob_append_latency_..._aws30m_primary_..._oplog_..._count{source="data01"} 18312.0
partition_blob_append_latency_..._aws30m_primary_..._page1_..._count{source="data01"} 4892.0
```

## Follow-Up

1. Fix TemporalAggregate deployed-client/deployed-server mismatch:
   - Reproduce with `temporal_aggregate_scale_benchmark`.
   - Trace `Internal: Request server resp size check failed`.
   - Verify whether the client package and server artifact were built from the same protocol/module revision.
2. Fix secondary table creation:
   - The two-replica table hit `Missing condition info` during readonly load.
   - Rerun secondary replication lag only after the table reaches `TABLE_NORMAL`.
3. Rerun true high-cardinality TemporalAggregate scale after those two issues are fixed.
