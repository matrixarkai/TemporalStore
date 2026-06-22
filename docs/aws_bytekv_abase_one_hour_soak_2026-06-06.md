# AWS ByteKV and ABase One-Hour Soak Test - 2026-06-06

This note records the one-hour ByteKV and ABase soak tests on the shared AWS
one-cluster test environment.

## Cluster

| Role | Instance | Private IP | Type |
|---|---|---|---|
| meta / client / proxy | `i-003c930417f7ee609` | `10.70.1.79` | `t3.small` |
| data01 | `i-0724d90b323786546` | `10.70.1.163` | `c7i.large` |
| data02 | `i-096334bd8cc7ab259` | `10.70.1.202` | `c7i.large` |

Post-test service health check passed: ByteKV `kvmaster`, `tso`, `kvproxy`,
and both `partitionserver` processes were still running. ABase `abase-master`,
`abase-proxy`, and both `abase-datanode` processes were also still running.

## ByteKV Soak

SSM command:

```text
c9e2c26a-4c10-48cf-a30e-07bd6661d541
```

Remote result directory:

```text
/var/lib/bytekv/soak_20260606T053439Z
```

Runtime:

| Field | Value |
|---|---|
| Start UTC | `2026-06-06T05:34:39Z` |
| Finish UTC | `2026-06-06T06:34:55Z` |
| Duration | 1 hour |
| Client host | `meta-01` |
| Master | `10.70.1.79:26010` |
| TSO | `10.70.1.79:26020` |
| Namespace | `aws_soak` |
| Per-iteration workload | 20,000 ops, 2 threads, 50/50 read/write, 128 B values |

Summary:

| Metric | Value |
|---|---:|
| Benchmark rows | 87 |
| Total ops | 1,740,000 |
| Reads | 870,000 |
| Writes | 870,000 |
| Operation errors reported by benchmark | 0 |
| Average QPS | 706.5 |
| Min QPS | 640.1 |
| Max QPS | 787.5 |
| Average p50 | 3.207 ms |
| Average p95 | 5.987 ms |
| Average p99 | 8.891 ms |
| Worst max latency seen | 397.834 ms |

Exit codes:

| Exit code | Count | Interpretation |
|---:|---:|---|
| 0 | 66 | clean benchmark process exit |
| 139 | 20 | segmentation fault after benchmark completion |
| 134 | 1 | abort after benchmark completion |

Finding:

ByteKV service behavior looked healthy: every benchmark row printed `PASS`,
and benchmark-level operation errors were `0`. The abnormality is in the
`bytekv_aws_smoke_soak` client process cleanup path: 21 of 87 iterations
completed the benchmark, printed a passing result, then crashed with
`SIGSEGV` or `SIGABRT`. This matches the earlier cleanup-segfault symptom and
should be treated as a client/tool bug until fixed. It did not stop the ByteKV
services.

## ABase Soak

SSM command:

```text
9c4d8afd-538e-4a20-a10b-0a0f459c1b45
```

Remote result directory:

```text
/var/lib/abase/soak_20260606T064636Z
```

Runtime:

| Field | Value |
|---|---|
| Start UTC | `2026-06-06T06:46:36Z` |
| Finish UTC | `2026-06-06T07:46:42Z` |
| Duration | 1 hour |
| Client host | `meta-01` |
| Master | `10.70.1.79:19074` |
| Table | `aws_scale/bench` |
| Client path | Direct master + datanode HTTP/brpc path |
| Threads | 2 |
| Batch size | 5 keys per write batch and 5 keys per read batch |

Important: ABase Redis/RESP proxy path was not used for this soak. Preflight
against ports `19077` and `19078` showed `redis-cli PING` timed out or closed
the connection. The working path was the direct local SDK style path:
master metadata lookup plus datanode `WriteBatch` / `MultiGet`.

Summary:

| Metric | Value |
|---|---:|
| Batch rows | 205,686 |
| Key pairs written and read | 1,028,430 |
| Write batch errors | 0 |
| Read/mismatch errors | 0 |
| Key pairs per second | 285.675 |
| Approx write keys/s | 285.675 |
| Approx read keys/s | 285.675 |

Write latency per 5-key batch:

| Metric | Value |
|---|---:|
| Average | 17.237 ms |
| p50 | 16.054 ms |
| p95 | 25.069 ms |
| p99 | 34.525 ms |
| Max | 1022.254 ms |

Read latency per 5-key batch:

| Metric | Value |
|---|---:|
| Average | 17.212 ms |
| p50 | 16.058 ms |
| p95 | 25.008 ms |
| p99 | 34.370 ms |
| Max | 1043.537 ms |

Finding:

The ABase direct path completed the full hour with `0` errors and no client
crashes. There were rare long-tail spikes around 1 second, but the normal p99
batch latency stayed around 34 ms on this small cluster.

The main unresolved ABase issue is the proxy protocol path: the service reports
a live proxy on `19077/19078`, but Redis/RESP commands did not work. The proxy
metadata says `ABASE2_THRIFT_PROTOCOL`, so Redis-compatible testing needs
either the correct Thrift client path or a separate RESP-compatible proxy mode.

## Comparison Notes

| System | Test path | Workload | Errors | Main abnormality |
|---|---|---|---:|---|
| ByteKV | `bytekv_aws_smoke_soak` through master/TSO/proxy routing | 50/50 read/write, 2 threads, 128 B values | 0 benchmark op errors | client process cleanup segfault/abort after successful rows |
| ABase | direct master + datanode HTTP/brpc | 5-key write batch plus 5-key read batch, 2 threads | 0 | Redis/RESP proxy path unavailable; direct path healthy |

These are soak tests on a small shared AWS cluster, not maximum-capacity
benchmarks. The results are useful for stability and abnormal-behavior checks.

## Follow-Ups

- Fix ByteKV `bytekv_aws_smoke_soak` cleanup crash, then rerun the same one-hour soak.
- Add a ByteKV soak mode that reuses one table instead of creating a new table per iteration.
- Confirm the correct ABase proxy client/protocol for `ABASE2_THRIFT_PROTOCOL`.
- If Redis-compatible ABase is required, start or configure a RESP-compatible proxy and rerun a one-hour proxy soak.
- Add data-node CPU sampling for ABase direct tests, because this run sampled meta/client-side processes only.
