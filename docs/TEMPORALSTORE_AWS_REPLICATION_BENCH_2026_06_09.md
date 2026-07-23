# TemporalStore AWS Replication Benchmark - 2026-06-09

## Cluster

This run reused the existing TemporalStore AWS test VPC and metaserver/client node, then launched two temporary data nodes for the benchmark.

| Role | Instance | Private IP | Public IP | Type |
|---|---|---:|---:|---|
| metaserver/client | `i-05f55360d92c43908` | `10.70.1.161` | `34.223.234.63` | `t3.small` |
| data01 | `i-0f9f971b0ef044adf` | `10.70.1.95` | `16.145.87.228` | `c7i.large` |
| data02 | `i-0655861cbc08cb926` | `10.70.1.167` | `16.148.47.138` | `c7i.large` |

Temporary shared store:

- EFS: `fs-0225c5875f9e4c910`
- Mounted on both data nodes at `/mnt/temporalstore-shared`
- Each data node also had a 10 GB gp3 cache volume mounted at `/mnt/temporalstore-cache`

Runtime artifact:

- `s3://temporalstore-test-artifacts-657817560042-us-west-2/temporalstore/temporalstore-release-raft-20260608.tar.gz`
- Installed binaries included `bcache2-server`, `bcache2-metaserver`, `bcache2-proxy`, `string_scale_benchmark`, `replication_smoke_example`, and `temporal_aggregate_scale_benchmark`.

## Harness Fix

The AWS benchmark harness needed two compatibility fixes before table creation worked:

- Include both `operator_name` and `operator` in request ids for compatibility with newer/older JSON handling.
- Escape Bash `$table_request` in the PowerShell-generated SSM payload. Before the fix, PowerShell expanded it to an empty string, so `AddTable` sent an empty body and metaserver returned `operator is required`.

File:

- `tools/workspace/aws_temporalstore_raft_vs_shared_test.ps1`

## Benchmark Shape

- Data nodes: 2 x `c7i.large`, 2 vCPU each.
- Client ran on the metaserver node.
- Thread list: `1`, `2`.
- Operations per run: `4000`.
- Value size: `128` bytes.
- Blockcache disabled to isolate replication/storage path.
- Replica serving was validated with `replication_smoke_example`, which forces a secondary visibility check after primary write.

## Shared Store, Async Storage

Mode:

- `--data_replication_mode=shared_store`
- `--storage_async=true`
- `--secondary_pull_stream_from_primary=false`
- Shared storage URI: `shared-file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260609_182608/`

Secondary visibility:

- Passed.
- Secondary read matched after `2` attempts, `104 ms`.

Results:

| Threads | Set QPS | Set p50 us | Set p95 us | Set p99 us | Get QPS | Get p50 us | Get p95 us | Get p99 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 2001 | 416 | 722 | 2270 | 2018 | 376 | 1113 | 2976 | 0 |
| 2 | 4395 | 365 | 624 | 2620 | 3072 | 354 | 2230 | 4015 | 0 |

CPU scrape after run:

| Node | Process CPU | Process Memory |
|---|---:|---:|
| data01 | `5.3%` | `1.4%` |
| data02 | `7.3%` | `1.5%` |

Interpretation:

- Async storage is the practical high-write-QPS shared-store path on this small EFS-backed test.
- The measured secondary visibility was about `104 ms` for the smoke write.
- Reads are replica-eligible after replay, but consistency is bounded by replay lag.

## Shared Store, Sync Storage

Mode:

- `--data_replication_mode=shared_store`
- `--storage_async=false`
- `--secondary_pull_stream_from_primary=false`
- Shared storage URI: `shared-file:///mnt/temporalstore-shared/aws-scale/storage/shared_store-20260609_182902/`

Secondary visibility:

- Passed.
- Secondary read matched after `2` attempts, `101 ms`.

Results:

| Threads | Set QPS | Set p50 us | Set p95 us | Set p99 us | Get QPS | Get p50 us | Get p95 us | Get p99 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | 98 | 9714 | 13648 | 17303 | 2300 | 346 | 771 | 2311 | 0 |
| 2 | 167 | 10562 | 19705 | 22992 | 4705 | 365 | 568 | 2045 | 0 |

CPU scrape after run:

| Node | Process CPU | Process Memory |
|---|---:|---:|
| data01 | `5.1%` | `1.4%` |
| data02 | `7.2%` | `1.5%` |

Interpretation:

- Sync storage is durable but dominated by EFS write latency in this setup.
- Reads stay fast because data is hot in memory and the read path does not pay the same sync write cost.
- This mode fits safer batch/materialized features, but not high write QPS on small EFS.

## Raft Consensus Path

Mode:

- `--data_replication_mode=raft_consensus`
- `--storage_async=false`
- `--data_raft_read_mode=bounded_stale`
- `--data_raft_bounded_stale_max_index_lag=16`
- Local storage URI: `file:///var/lib/temporalstore/raft-local/raft-20260609_183244/`

Result:

- Servers started on both data nodes.
- First primary write timed out.
- No valid Raft QPS, read QPS, or secondary lag number was produced.

Observed failure:

```text
FAIL primary set: Internal: Request server failed[E1008]Reached timeout=5000ms @10.70.1.95:17001 endpoint=10.70.1.95:17001
```

Log evidence:

- Metaserver continued sending `UpdateMembershipRequest` with growing `active_id_list` and many `frozen_id_list` entries.
- Data02 repeatedly failed derived partition loads:

```text
Setup condition failed ... Status:FailedPrecondition: Missing condition info
Failed to load partition ... Missing condition info
report load finish got error response ... partition state wrong
```

Interpretation:

- The Raft data-node servers can start, but the control plane still manages the table like the anti-entropy/shared-store path.
- It repeatedly creates/freeze-rotates derived partitions instead of owning one stable Raft replica group.
- Raft is still not production-ready and cannot be compared against shared-store QPS yet.

## Current Conclusion

| Mode | Status | 2-thread Write QPS | 2-thread Read QPS | Secondary Visibility | Notes |
|---|---|---:|---:|---:|---|
| shared-store async | Pass | `4395` | `3072` | `104 ms` | High-write path with RPO risk |
| shared-store sync | Pass | `167` | `4705` | `101 ms` | Safer but EFS-write-limited |
| Raft consensus | Fail | n/a | n/a | n/a | Control-plane Raft replica-group integration missing |

## Post-Test AWS State

After this benchmark, the temporary data nodes and shared EFS store were destroyed to avoid idle spend.

Remaining running resource:

| Role | Instance | Private IP | Public IP | Type | State |
|---|---|---:|---:|---|---|
| metaserver/client/company website | `i-05f55360d92c43908` | `10.70.1.161` | `34.223.234.63` | `t3.small` | running |

Confirmed destroyed or absent:

- Data node `i-0f9f971b0ef044adf`.
- Data node `i-0655861cbc08cb926`.
- EFS `fs-0225c5875f9e4c910`.
- EFS security group `sg-02e544a04d04d2298`.

Remaining storage:

- One 20 GiB gp3 root EBS volume attached to the metaserver/client node: `vol-07b2c066714e8b418`.

The live public endpoints are served from the remaining node:

- Company site: `https://matrixark.ai/`
- Observation UI: `https://matrixark.ai/observation/`
- Monitoring UI: `https://matrixark.ai/monitoring/`

## Next Engineering Work

To make Raft benchmarkable and eventually production-ready:

1. Add a first-class Raft table/partition relation, separate from anti-entropy.
2. Make metaserver create one stable Raft group per logical shard.
3. Store voters, learners, leader, term, epoch, and read policy in metaserver metadata.
4. Stop derived partition creation for Raft tables.
5. Implement real partition snapshot payload and install-snapshot flow.
6. Validate primary-down failover before AWS scale benchmarking.
7. Re-run the same matrix:
   - shared-store async
   - shared-store sync
   - Raft leader-read
   - Raft bounded-stale replica-read
