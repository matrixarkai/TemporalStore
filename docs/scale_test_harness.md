# Scale Test Harness

`scale_harness` is an in-process TemporalStore Rust scale/fault harness for fast local and CI
coverage. It exercises the same Rust Raft/data APIs as the unit and compatibility tests, without
requiring a deployed cluster.

It covers:

- Raft write replication across multiple voters
- sampled follower reads with read-index lag checks
- latency summaries for follower reads served by replicas
- optional shared-store sync/async replay comparison with replica-read latency and lag
- repeated leader transfer/failover
- safe scale up and scale down
- string and hash write volume
- long sequence feature writes and filtered reads
- final replication-health verification with max lag `0`

Quick smoke:

```bash
TS_SCALE_STRING_OPS=40 \
TS_SCALE_HASH_OPS=10 \
TS_SCALE_SEQUENCE_KEYS=2 \
TS_SCALE_SEQUENCE_LEN=100 \
TS_SCALE_EVENTS=2 \
TS_SCALE_FAILOVER_EVERY=10 \
TS_SCALE_READ_SAMPLE_EVERY=10 \
tools/run_temporalstore_scale_harness.sh
```

The wrapper uses release mode by default. Set `TS_SCALE_PROFILE=debug` only when debugging the
harness itself.

Heavier local profile:

```bash
TS_SCALE_NODES=5 \
TS_SCALE_STRING_OPS=1000 \
TS_SCALE_HASH_OPS=250 \
TS_SCALE_SEQUENCE_KEYS=8 \
TS_SCALE_SEQUENCE_LEN=1000 \
TS_SCALE_EVENTS=8 \
TS_SCALE_FAILOVER_EVERY=100 \
tools/run_temporalstore_scale_harness.sh
```

Direct binary usage:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes 3 \
  --string-ops 1000 \
  --hash-ops 250 \
  --sequence-keys 4 \
  --sequence-len 500 \
  --scale-events 2 \
  --failover-every 250 \
  --read-sample-every 100
```

Raft replica reads versus shared-store replication comparison:

```bash
TS_SCALE_COMPARE_SHARED_STORE=true \
TS_SCALE_NODES=3 \
TS_SCALE_STRING_OPS=1000 \
TS_SCALE_HASH_OPS=250 \
TS_SCALE_SEQUENCE_KEYS=4 \
TS_SCALE_SEQUENCE_LEN=500 \
TS_SCALE_SHARED_STORE_OPS=1000 \
TS_SCALE_SHARED_STORE_FLUSH_EVERY=25 \
tools/run_temporalstore_scale_harness.sh
```

The JSON output includes:

- `raft_replica_read_latency`: p50/p95/p99/max latency for reads served by Raft replicas
- `max_replica_lag`: final Raft replica lag, expected to be `0`
- `raft_node_statuses`: per-node role, replica role, commit index, applied index, alive state, and
  lag against the leader commit index
- `shared_store.sync_replica_read_latency`: latency after sync shared-store publish and replay
- `shared_store.async_replica_read_latency`: latency for reads after async flush/replay windows
- `shared_store.async_storage_enqueue_latency`: response-path cost to queue async shared-store work
- `shared_store.async_storage_flush_latency`: background flush cost to publish queued async oplog entries
- `shared_store.sync_max_lag` and `shared_store.async_max_lag`: max oplog lag observed while replaying

For `storage_async=true` parity with the C++ path, do not read
`async_storage_enqueue_latency` as durable-storage latency. C++ `Partition::OnExecuteCmdDone`
returns before `op_logger_->Commit` for `PERSISTENT_ASYNC` when `FLAGS_storage_async=true`, so the
client-visible write latency mostly covers command execution plus async enqueue/scheduling. Durable
shared-store cost is paid by the background commit/flush path. The Rust harness therefore reports
both the enqueue-side latency and the actual async flush latency.

## More Data-Node Replica Profile

Use this profile when testing secondary lag, replica-read latency, leader transfer, failover, safe
scale-up, and safe scale-down with more Raft data-node replicas:

```bash
tools/run_temporalstore_more_data_nodes.sh
```

Defaults:

- `TS_MORE_NODES=7`
- `TS_MORE_NODES_STRING_OPS=2000`
- `TS_MORE_NODES_HASH_OPS=500`
- `TS_MORE_NODES_SEQUENCE_KEYS=4`
- `TS_MORE_NODES_SEQUENCE_LEN=1000`
- `TS_MORE_NODES_SCALE_EVENTS=6`
- `TS_MORE_NODES_FAILOVER_EVERY=250`
- `TS_MORE_NODES_READ_SAMPLE_EVERY=10`
- `TS_MORE_NODES_COMPARE_SHARED_STORE=true`
- `TS_MORE_NODES_SHARED_STORE_OPS=2000`
- `TS_MORE_NODES_SHARED_STORE_FLUSH_EVERY=20`

For a larger local or EC2-hosted in-process run:

```bash
TS_MORE_NODES=9 \
TS_MORE_NODES_STRING_OPS=5000 \
TS_MORE_NODES_HASH_OPS=1000 \
TS_MORE_NODES_SEQUENCE_KEYS=8 \
TS_MORE_NODES_SEQUENCE_LEN=1000 \
TS_MORE_NODES_SCALE_EVENTS=10 \
tools/run_temporalstore_more_data_nodes.sh
```

The important fields to inspect are:

- `raft_node_statuses[*].lag`
- `raft_node_statuses[*].commit_index`
- `raft_node_statuses[*].applied_index`
- `raft_replica_read_latency`
- `shared_store.async_max_lag`
- `shared_store.async_storage_flush_latency`

This is still an in-process data-node replica test. It is useful for Raft correctness, secondary lag,
and replica-read regression coverage, but it does not replace a true multi-EC2 data-node test with
real network, EBS, and EFS paths.

## Client Scale Harness

`client_scale_harness` focuses on the Rust client library and the proxy/client serving path. It
starts local HTTP data-node servers plus a tiny metaserver route service, creates multiple
`TemporalStoreClient` instances, opens a sharded table, and drives concurrent write/read/batch
traffic through `TemporalStoreTable`.

Example:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin client_scale_harness -- \
  --clients 8 \
  --threads-per-client 4 \
  --ops-per-thread 1000 \
  --shards 16 \
  --servers 4 \
  --read-every 2 \
  --batch-every 50
```

The JSON output includes:

- `ops_per_sec`: successful client operations per second across writes, sampled reads, and batches
- `write_latency`, `read_latency`, `batch_latency`: p50/p95/p99/max latency summaries
- `aggregate_client_stats.route_cache_hits/misses/refreshes`: client route-cache behavior
- `aggregate_client_stats.backend_errors`: data-node call failures observed by clients
- `route_cache_size_total`: summed route-cache entries across all client instances

Use this harness after changing `client.rs`, `proxy.rs`, route-cache behavior, or table key-to-shard
routing. It is intentionally process-local for fast iteration; AWS validation can run the same
binary inside a larger EC2/EKS job once real multi-process data-node deployment is available.

AWS/EKS usage:

The existing Terraform in `infra/aws-existing-eks` still deploys one stateful server, so it can scale
the stateless proxy and Redis layers but cannot honestly measure distributed data-node replica-read
latency. To run the replica-read/shared-store comparison on AWS today, run this harness inside an EC2
instance or EKS job image built from the repo:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-target \
cargo run --release -p temporalstore-rust --bin scale_harness -- \
  --nodes 5 \
  --string-ops 5000 \
  --hash-ops 1000 \
  --sequence-keys 8 \
  --sequence-len 1000 \
  --scale-events 4 \
  --failover-every 500 \
  --read-sample-every 50 \
  --compare-shared-store true \
  --shared-store-ops 5000 \
  --shared-store-flush-every 50
```

Long-sequence Raft usage:

```bash
CARGO_TARGET_DIR=/tmp/temporalstore-rust-target \
cargo run -p temporalstore-rust --bin scale_harness -- \
  --nodes 3 \
  --string-ops 120 \
  --hash-ops 40 \
  --sequence-keys 1 \
  --sequence-len 5000 \
  --scale-events 2 \
  --failover-every 40 \
  --read-sample-every 20
```

The harness prints a JSON summary and exits non-zero if final replication health fails.

## C++ p99 Gate

Use `tools/run_temporalstore_cpp_p99_gate.sh` when validating Rust steady-state serving latency
against the C++ data-type smoke document from
`benchmarks/data-type-trace-2026-05-27-r2/README.md`.

The C++ document reports local smoke measurements with only 5 operations per case, primary-pinned
reads, 2 metaservers, 2 data servers, replica count 2, and local file storage. To keep the comparison
apples-to-apples, the Rust gate runs the scale harness in steady state by default:

- `--scale-events 0`
- `--failover-every 0`
- `--read-sample-every 1`
- shared-store comparison enabled

Default p99 targets are copied from the C++ table:

| Rust metric | C++ row used | Target |
|---|---|---:|
| `raft_replica_read_latency.p99_us` | `FEATURE query_window_one_point` | `1593 us` |
| `shared_store.sync_primary_write_latency.p99_us` | `STRING ingest_set` | `15695 us` |
| `shared_store.async_primary_write_latency.p99_us` | `STRING ingest_set` | `15695 us` |
| `shared_store.sync_replica_read_latency.p99_us` | `STRING query_get` | `1353 us` |
| `shared_store.async_replica_read_latency.p99_us` | `STRING query_get` | `1353 us` |

The Raft read target uses the C++ feature-query p99 because the Rust aggregate includes both string
replica reads and sequence-filter replica reads. Override thresholds with:

```bash
TS_CPP_P99_RAFT_READ_US=1593 \
TS_CPP_P99_SYNC_WRITE_US=15695 \
TS_CPP_P99_ASYNC_WRITE_US=15695 \
TS_CPP_P99_SYNC_READ_US=1353 \
TS_CPP_P99_ASYNC_READ_US=1353 \
tools/run_temporalstore_cpp_p99_gate.sh
```

On the Windows-mounted local workspace, the in-process Raft/engine model is intentionally
correctness-heavy and writes local page/index data. Treat the output as a local regression signal
for scale/failover behavior, not as production throughput.

The default Raft config still exposes the ByteRaft-style `32 KiB` max memory replicate-log entry
limit. A 5k-row sequence command is larger than that in the Rust JSON command shape, so the Raft
proposal path now chunks `SequenceAdd` commands into ordered smaller entries. `--max-log-entry-bytes`
is still available for stress testing and for future command shapes where one logical row is larger
than the default entry limit. Production still needs a transactional multi-entry policy if callers
require all-or-nothing semantics across a chunked logical append.

This is not a substitute for multi-process chaos testing. It is intended as a fast regression gate
for scale/failover logic while the production Raft runtime is still being hardened.
