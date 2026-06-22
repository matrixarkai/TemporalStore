# Secondary Replication Visibility Lag Test

Date: 2026-06-04 Pacific / 2026-06-05 UTC

## Goal

Measure actual secondary visibility lag with no fixed sleep:

1. Write a unique STRING key to the primary.
2. Start the lag timer after primary `Set` returns.
3. Tight-poll the secondary until the exact key/value is visible.
4. Record microsecond latency and polling attempts.

This is different from the older `replication_smoke_example`, which sleeps 100 ms between retries and therefore can report coarse 100 ms-ish lag even when real visibility is much lower.

## AWS Setup

- Region: `us-west-2`
- Metaserver/proxy node: `t3.small`, private `10.70.1.79`
- Data nodes: 2 x `c7i.large`, 2 vCPU each
- Primary: `10.70.1.163`
- Secondary: `10.70.1.202`
- Metaserver endpoint: `10.70.1.79:17000`
- Namespace/table: `ns1/table1`
- Shared persistence path: EFS-backed shared-file path
- Client benchmark host: metaserver node

## Benchmark Tool

New tool:

```bash
/opt/temporalstore/bin/secondary_visibility_lag_benchmark \
  <metaserver_host:port> <idc> <namespace> <table> \
  [probe_ops] [probe_threads] [value_bytes] [max_wait_ms] \
  [background_writer_threads] [background_reader_threads]
```

The tool opens:

- A primary-pinned writer table.
- A secondary-affinity reader table using `force-secondary-read`.
- Optional background primary writers.
- Optional background secondary readers.

It reports:

- `secondary_visibility_lag_after_primary_set`: elapsed time from primary `Set` success to exact secondary `Get` match.
- `secondary_visibility_poll_attempts`: tight polling attempts needed to observe visibility.
- Background write/read operation counts while the probe is running.

## Results

| Case | Probe ops | Probe threads | Background writers | Background readers | Background write QPS | Background read QPS | Errors | Avg lag | P50 lag | P95 lag | P99 lag | Max lag | Poll attempts P99 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Idle | 50 | 1 | 0 | 0 | 0/s | 0/s | 0 | 1.101 ms | 0.655 ms | 3.653 ms | 4.983 ms | 4.983 ms | 2 |
| Load 2w/2r | 100 | 1 | 2 | 2 | 182/s | 355/s | 0 | 8.340 ms | 9.355 ms | 15.740 ms | 20.082 ms | 21.830 ms | 2 |
| Load 4w/4r | 100 | 1 | 4 | 4 | 362/s | 683/s | 0 | 8.935 ms | 9.446 ms | 18.325 ms | 19.257 ms | 24.920 ms | 2 |
| Load 8w/8r | 200 | 2 | 8 | 8 | 691/s | 1314/s | 0 | 9.097 ms | 9.420 ms | 18.649 ms | 20.828 ms | 21.117 ms | 2 |

## Interpretation

- Idle secondary visibility is sub-millisecond at p50 and under 5 ms at p99/max for this run.
- Concurrent writes and secondary reads push visibility into the roughly 9 ms p50 and 20 ms p99 band.
- The secondary usually sees the value on the first or second tight poll.
- The load tests did not show unbounded lag for STRING replication on this 2-node EFS setup.
- These numbers are for exact key/value visibility after acknowledged primary write, not raw secondary read latency and not aggregate-query recomputation latency.

## Caveats

- The data nodes are small 2-vCPU instances. Higher write/read load should also be tested on larger instances.
- This run measured STRING visibility. TemporalAggregate replay/visibility should have a matching module-specific benchmark because module write replay previously showed separate bugs.
- Background load duration is tied to probe completion time, so the background operation counts are useful for pressure indication but not a long-duration saturation test.
- EFS durability and shared-file behavior are part of this test; local NVMe primary-pull mode would need a separate run.
