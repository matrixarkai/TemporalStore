# AWS Shared-Store STRING QPS Test - 2026-06-08

## Scope

This run measured TemporalStore plain STRING write/read QPS on the existing AWS one-cluster deployment.

This is a successful shared-store test, not a Raft production test. The data-node Raft path is still guarded until real partition snapshot/install-snapshot and failover validation are complete.

## Cluster

| Role | Instance | Type | Private IP |
| --- | --- | --- | --- |
| Metaserver / client | `i-05f55360d92c43908` | `t3.small` | `10.70.1.161` |
| Data node 1 | `i-0cfbef56e86551535` | `c7i.large` | `10.70.1.214` |
| Data node 2 | `i-04c93ad8271e5b64a` | `c7i.large` | `10.70.1.24` |

Data nodes were started with:

- `--data_replication_mode=shared_store`
- `--secondary_pull_stream_from_primary=false`
- `--storage_async=true`
- storage URI: `shared-file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260608_032345/`
- `--stream_max_blob_size=268435456`
- `--storage_zone_size=268435456`
- blockcache disabled for this run

## Replication Smoke

Result:

```text
PASS replication smoke: secondary read matched after 2 attempts, 101 ms
```

## STRING QPS

Benchmark command shape:

```bash
/opt/temporalstore/bin/string_scale_benchmark 10.70.1.161:17000 vdc1 <namespace> tbl 4000 <threads> 128 1 1000
```

Each row used 4,000 set ops followed by 4,000 get ops with 128-byte values.

| Threads | Set QPS | Set p50 us | Set p95 us | Set p99 us | Get QPS | Get p50 us | Get p95 us | Get p99 us | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 3,460 | 251 | 401 | 1,060 | 3,633 | 250 | 396 | 561 | 0 |
| 2 | 4,352 | 246 | 1,996 | 3,965 | 5,899 | 221 | 1,057 | 2,146 | 0 |
| 4 | 9,345 | 347 | 681 | 1,535 | 10,810 | 311 | 578 | 696 | 0 |
| 8 | 19,900 | 339 | 593 | 1,105 | 20,725 | 330 | 547 | 877 | 0 |

The benchmark also emitted `get_visibility_retry` rows. In this run those matched raw successful get throughput and had zero errors, so the table above reports raw successful get latency.

## CPU Snapshot

After the run:

| Node | `bcache2-server` CPU | Memory |
| --- | ---: | ---: |
| data01 | 5.7% | 1.6% |
| data02 | 7.3% | 1.7% |

These are post-run process snapshots, not peak CPU during the benchmark. A longer run should collect per-second CPU if we want saturation curves.

## Local Test Status

Local WSL has server binaries under `/home/vj/temporalstore-native/output-ubuntu22/release-coherent`, but the matching `string_scale_benchmark` and `replication_smoke_example` client binaries were not found in the local release folders checked. Because the clean Git repo intentionally excludes dependencies and release artifacts, I did not rebuild locally in this pass.

To run the same local QPS test, the next step is to build or stage the client benchmark binaries beside the local server/metaserver release folder.

## Harness Fix

The first AWS run failed before starting data nodes because SSM decoded PowerShell-generated scripts with CRLF line endings, which broke bash heredoc terminators. The harness now decodes scripts through:

```bash
base64 -d | tr -d '\r'
```

The benchmark parser was also updated to accept the newer `get_raw_success_attempt` phase name.
