# TemporalStore Local Docker Replication Matrix - 2026-06-10

## Scope

This run validates local Ubuntu 22 Docker execution of TemporalStore release artifacts across:

- shared-store with async storage
- shared-store with sync storage
- data-node Raft replication

Each mode used 3 data nodes and 1 metaserver. Primary write/read QPS used 8,000 STRING operations per pass with 256-byte values. Replica-eligible read passes used 100 operations because shared-store secondary visibility in this local Docker/file-backed setup is slow enough that large secondary sweeps dominate the test wall clock.

## Commands

```bash
cmake --build build-ubuntu22/release \
  --target bcache2-server bcache2-metaserver \
           string_scale_benchmark replication_smoke_example \
           secondary_visibility_lag_benchmark \
  --parallel 1

RESULT_DIR=/tmp/ts-docker-matrix-full \
OPS=8000 \
REPLICA_OPS=100 \
THREAD_LIST=2,4 \
VALUE_BYTES=256 \
BASE_PORT=15000 \
BENCH_TIMEOUT_S=1200 \
RUN_FAILOVER=1 \
bash tools/run_local_docker_replication_matrix_ubuntu22.sh
```

The Docker harness uses a named Docker volume instead of a bind mount because this WSL Docker environment maps direct WSL bind mounts incorrectly. The script copies release artifacts into the volume, runs the matrix inside Docker with host networking, and copies `/workspace/results` back to the requested `RESULT_DIR`.

## Write And Read QPS

| Mode | Threads | Read policy | Phase | QPS | p50 us | p95 us | p99 us | Errors |
|---|---:|---|---|---:|---:|---:|---:|---:|
| shared_async | 2 | primary | set | 5390 | 319 | 618 | 865 | 0 |
| shared_async | 2 | primary | get raw | 6359 | 287 | 481 | 618 | 0 |
| shared_async | 4 | primary | set | 7455 | 508 | 787 | 1002 | 0 |
| shared_async | 4 | primary | get raw | 6728 | 547 | 969 | 1366 | 0 |
| shared_sync | 2 | primary | set | 483 | 3504 | 7096 | 8465 | 0 |
| shared_sync | 2 | primary | get raw | 4415 | 416 | 734 | 1079 | 0 |
| shared_sync | 4 | primary | set | 874 | 4481 | 7141 | 8145 | 0 |
| shared_sync | 4 | primary | get raw | 9367 | 410 | 604 | 728 | 0 |
| raft | 2 | primary | set | 80 | 23092 | 32848 | 96514 | 0 |
| raft | 2 | primary | get raw | 4232 | 424 | 768 | 1192 | 0 |
| raft | 4 | primary | set | 74 | 49905 | 79369 | 160716 | 0 |
| raft | 4 | primary | get raw | 5763 | 608 | 1181 | 2048 | 0 |

## Replica-Eligible Read Check

These runs write 100 keys and then allow reads to route to replica-eligible partitions. They are not the main read-QPS number; they are a bounded secondary replication/routing check.

| Mode | Threads | Phase | QPS | p50 us | p95 us | p99 us | Errors |
|---|---:|---|---:|---:|---:|---:|---:|
| shared_async | 2 | get visibility retry | 2 | 1001126 | 1002323 | 1002663 | 0 |
| shared_async | 4 | get visibility retry | 4 | 1001220 | 1002539 | 1004591 | 0 |
| shared_sync | 2 | get visibility retry | 1 | 2001820 | 2003520 | 2006137 | 0 |
| shared_sync | 4 | get visibility retry | 2 | 2002285 | 2003538 | 2003709 | 0 |
| raft | 2 | get visibility retry | 2857 | 499 | 956 | 1107 | 0 |
| raft | 4 | get visibility retry | 4761 | 451 | 894 | 1087 | 0 |

## Secondary Visibility Probe

The tight secondary visibility probe runs 100 primary writes and polls for secondary visibility.

| Mode | Threads | Samples | Avg us | p50 us | p95 us | p99 us | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| shared_async | 2 | 100 | 944043 | 1000616 | 1001524 | 1001845 | 0 |
| shared_async | 4 | 100 | 961311 | 1000645 | 1001986 | 1002388 | 0 |
| shared_sync | 2 | 100 | 1720703 | 1987815 | 2000101 | 2000541 | 0 |
| shared_sync | 4 | timed out | - | - | - | - | - |
| raft | 2 | 100 | 1551 | 907 | 4839 | 7684 | 0 |
| raft | 4 | 100 | 1453 | 999 | 4647 | 5749 | 0 |

The shared-sync 4-thread visibility probe hit the 180-second timeout. Primary write/read QPS for the same case completed with zero errors.

## Raft Failover

Raft failover passed in Docker.

The failover harness:

1. Started a 3-node data-Raft cluster.
2. Verified baseline replica read.
3. Ran a tight secondary visibility benchmark.
4. Killed the original primary server.
5. Observed metaserver promotion to `1099511693312`.
6. Verified post-failover write/read.

Post-failover result:

| Phase | QPS | p50 us | p95 us | p99 us | Errors |
|---|---:|---:|---:|---:|---:|
| set | 82 | 23739 | 31264 | 33976 | 0 |
| get raw | 3278 | 468 | 772 | 1340 | 0 |

## Snapshot And Raft Artifacts

The Docker run produced data-Raft local stream, applied-index, and WAL files for raft scale and failover cases:

- `data-raft/local-streams/<partition>/partition-*`
- `data-raft/applied/<partition>`
- `data-raft/wal/<partition>/0000000000000001.seg`

No fatal/assertion logs were found after the stream-buffer fix. Data-node Raft snapshot creation/loading exists in the partition path, but there is not yet a direct data-node snapshot trigger RPC in this harness. Deterministic snapshot-install testing remains a follow-up.

## Fix Found During Testing

The first Docker stress pass crashed a data node in `StreamBuffer::GetFrontData()` when async flush observed a front offset at or beyond a chunk boundary. The fix:

- skips fully consumed chunks before copying front data
- adds `CanReadFront(size)` as a non-mutating guard
- makes `TryAppend()` skip a stale/unreadable front range with a warning instead of aborting the server

A repeat stress pass found a second race under shared-sync writes: async append completion could trim the stream buffer while another path was reading the same `RingArray`, causing `ring_array.h:63` to abort the data node. The follow-up fix serializes stream buffer and offset mutation paths with a stream-level mutex around append, commit, flush scheduling, and append completion.

## Repeat Stress Pass

After the stream mutex fix, the Docker matrix was repeated with 10,000 primary operations per case, 100 replica-eligible operations, 2 and 4 client threads, and raft failover enabled.

| Mode | Threads | Primary SET QPS | Primary GET QPS | Errors |
|---|---:|---:|---:|---:|
| shared_async | 2 | 4376 | 5574 | 0 |
| shared_async | 4 | 6116 | 6784 | 0 |
| shared_sync | 2 | 479 | 4399 | 0 |
| shared_sync | 4 | 764 | 7507 | 0 |
| raft | 2 | 73 | 4098 | 0 |
| raft | 4 | 77 | 6993 | 0 |

Repeat raft failover also passed:

| Phase | QPS | p50 us | p95 us | p99 us | Errors |
|---|---:|---:|---:|---:|---:|
| set | 78 | 22825 | 42620 | 59182 | 0 |
| get raw | 2666 | 608 | 1099 | 1366 | 0 |

No fatal or assertion logs were found in the repeat-fixed artifacts.

## Host-Local Raft Failover And Scale Up/Down

Additional host-local raft tests were run after the Docker matrix to isolate data-node Raft membership behavior without Docker bind-mount or port mapping noise.

Commands:

```bash
RESULT_DIR=/tmp/ts-raft-2node-scale-seq \
OPS=4000 THREAD_LIST=2,4 VALUE_BYTES=128 BENCH_TIMEOUT_S=300 \
MS_PORT=34100 SERVER_PORT=17101 \
bash tools/run_data_raft_2node_scale_ubuntu22.sh

SMOKE_DIR=/tmp/ts-raft-failover-seq3 \
RUN_LOG_DIR=/tmp/ts-raft-failover-seq3-runner \
MS_PORT=20000 SERVER_PORT=18101 \
bash tools/run_data_raft_failover_ubuntu22.sh

RESULT_DIR=/tmp/ts-raft-scale-up-down-seq \
OPS=800 THREADS=2 VALUE_BYTES=128 BENCH_TIMEOUT_S=240 \
MS_PORT=21000 SERVER_PORT=19101 \
bash tools/run_data_raft_scale_up_down_ubuntu22.sh
```

The 2-node raft scale run passed baseline replica-read smoke and primary write/read benchmarks:

| Threads | SET QPS | SET p50 us | SET p95 us | GET QPS | GET p50 us | GET p95 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 90 | 21915 | 30125 | 3703 | 480 | 821 | 0 |
| 4 | 77 | 50261 | 64321 | 5714 | 622 | 1202 | 0 |

The failover run passed baseline replica-read smoke, tight secondary visibility, metaserver promotion, and post-failover write/read validation. The killed primary was frozen, `1099511693312` was promoted, and post-failover writes succeeded on the first attempt in 3648 ms.

| Phase | QPS | p50 us | p95 us | p99 us | Errors |
|---|---:|---:|---:|---:|---:|
| set | 58 | 31146 | 55057 | 80622 | 0 |
| get raw | 3278 | 495 | 980 | 1185 | 0 |

The new scale-up/down harness starts a 2-replica data-Raft table, adds a third server, updates the table from 2 to 3 replicas, validates writes/reads, updates the table back to 2 replicas, freezes and drops the third server, and validates writes/reads again.

| Stage | SET QPS | SET p50 us | SET p95 us | GET QPS | GET p50 us | GET p95 us | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline 2 replicas | 68 | 26608 | 44089 | 3619 | 481 | 897 | 0 |
| after scale up to 3 | 63 | 28739 | 44201 | 3636 | 481 | 892 | 0 |
| after scale down to 2 | 61 | 28768 | 49569 | 4000 | 443 | 698 | 0 |
| after dropping server 3 | 63 | 28415 | 42649 | 3921 | 449 | 785 | 0 |

Repeat testing also caught a startup hazard: occupied metaserver or data-Raft transport ports could make the binary fail during bind, and one metaserver startup attempt segfaulted after failing to listen on its raft port. The raft scripts now preflight metaserver client, raft, snapshot, data-node service, data-Raft, and data-Raft snapshot ports before starting processes.

The raft host-local suite was repeated with fresh result directories and non-overlapping ports:

```bash
RESULT_DIR=/tmp/ts-raft-2node-scale-repeat2 \
OPS=4000 THREAD_LIST=2,4 VALUE_BYTES=128 BENCH_TIMEOUT_S=300 \
MS_PORT=22000 SERVER_PORT=22101 \
bash tools/run_data_raft_2node_scale_ubuntu22.sh

SMOKE_DIR=/tmp/ts-raft-failover-repeat2 \
RUN_LOG_DIR=/tmp/ts-raft-failover-repeat2-runner \
MS_PORT=23000 SERVER_PORT=23101 \
bash tools/run_data_raft_failover_ubuntu22.sh

RESULT_DIR=/tmp/ts-raft-scale-up-down-repeat3 \
OPS=800 THREADS=2 VALUE_BYTES=128 BENCH_TIMEOUT_S=240 \
MS_PORT=25000 SERVER_PORT=25101 \
bash tools/run_data_raft_scale_up_down_ubuntu22.sh
```

Repeat 2-node scale passed:

| Threads | SET QPS | SET p50 us | SET p95 us | GET QPS | GET p50 us | GET p95 us | Errors |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2 | 87 | 22549 | 31408 | 4338 | 403 | 737 | 0 |
| 4 | 95 | 41753 | 55255 | 6493 | 570 | 953 | 0 |

Repeat failover passed with promotion to `1099511693312`; post-failover writes succeeded on the first attempt in 2333 ms:

| Phase | QPS | p50 us | p95 us | p99 us | Errors |
|---|---:|---:|---:|---:|---:|
| set | 91 | 22235 | 27714 | 32836 | 0 |
| get raw | 4545 | 367 | 475 | 693 | 0 |

The repeated scale-up/down run exposed one harness issue: after freezing server 3, the server can already be gone by the time the explicit `DropServer` request is sent. The harness now treats `DropServer` `NotFound` as idempotent success and still waits for server 3 to be absent before passing. After that fix, scale-up/down passed again:

| Stage | SET QPS | SET p50 us | SET p95 us | GET QPS | GET p50 us | GET p95 us | Errors |
|---|---:|---:|---:|---:|---:|---:|---:|
| baseline 2 replicas | 91 | 21580 | 26699 | 7079 | 267 | 336 | 0 |
| after scale up to 3 | 85 | 23516 | 28869 | 6106 | 309 | 404 | 0 |
| after scale down to 2 | 96 | 19480 | 25789 | 6611 | 284 | 367 | 0 |
| after dropping server 3 | 100 | 17809 | 25454 | 5673 | 315 | 496 | 0 |

No fatal, assertion, data-Raft startup, replicator setup, or SET failure logs were found in the repeated host-local artifacts.

## Takeaways

Shared async is the strongest write-QPS mode in this local Docker shape.

Shared sync is much slower for writes because it pays synchronous file-backed persistence cost, but primary reads remain fast.

Raft has the strongest secondary visibility and failover behavior, but write QPS is much lower because every write goes through consensus. The Docker failover path passed, and raft local WAL/applied artifacts were present.
