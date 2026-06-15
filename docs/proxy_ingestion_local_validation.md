# Proxy Ingestion Local Validation

## Purpose

This is the local Ubuntu 22 validation path for the default TemporalStore ingestion model:
clients write through `bcache2-proxy`, the proxy routes requests to data nodes, and the local
shared-file cluster proves writes, reads, replication visibility, and proxy pressure behavior before
code is pushed.

The default public/customer path is proxy ingestion. Direct SDK writes remain useful for tightly
controlled low-latency internal services, and queue-backed ingestion workers can be layered on top
for replay, burst smoothing, and no-data-loss pipelines. The local test here focuses on the proxy
path because it is the easiest and safest default for users.

## Queue-Backed Ingestion For Kafka And Flink

Kafka, Flink, Pulsar, Kinesis, and Pub/Sub integrations should feed the same proxy/ingestion-worker
write path instead of bypassing TemporalStore routing and quota controls. The connector contract is:

- Read records with a stable source, partition, and offset.
- Deduplicate by `(source, partition, offset)` before writing.
- Batch records up to a bounded size.
- Retry failed writes with bounded backoff.
- Preserve proxy-side auth, tenant quota, routing, and metrics.

The local C++ parity gate is `queue_ingestion_replay_example`. It validates the connector replay
contract without requiring a live broker or proxy:

- `--source=api|kafka|flink`: validates direct API replay and the two connector modes separately.
- `--dry_run=1`: no external Kafka/Flink/proxy dependency; validates batching, dedupe, retry accounting,
  checkpoint/watermark accounting, dead-letter handling, and replay determinism at high local record counts.
- `--fail_first_attempt_every=N`: injects deterministic retryable write failures.
- `--dead_letter_every=N`: injects deterministic malformed records and verifies they do not block good
  records in the same replay.

Use `proxy_ingestion_pressure_example` and `proxy_smoke_example` for live proxy RPC validation. A real
Kafka/Flink connector should combine this replay contract with the same proxy write path.

Run the 8-iteration local replay gate:

```bash
ITERATIONS=8 \
RECORDS=5000 \
BATCH_SIZE=256 \
DUPLICATE_EVERY=17 \
SOURCES='api kafka flink' \
FAIL_FIRST_ATTEMPT_EVERY=53 \
DEAD_LETTER_EVERY=97 \
bash tools/run_queue_ingestion_replay_ubuntu22.sh
```

Expected output:

```text
PASS queue ingestion replay
iterations=8
records=5000
batch_size=256
duplicate_every=17
sources=api kafka flink
```

This is the local stand-in for Kafka/Flink connector pressure before adding real broker dependencies
to CI. A real connector should map Kafka topic/partition/offset or Flink checkpointed source offsets
into the same record identity and replay contract.

## Ingestion Gap Plan

| Area | Current local coverage | Remaining production gap |
| --- | --- | --- |
| Direct APIs | `source=api` replay plus live proxy pressure. | Add customer SDK examples for every module API shape. |
| Kafka ingestion | Deterministic source/partition/offset replay, dedupe, retry, dead-letter, and checkpoint metrics. | Add optional librdkafka-based worker when broker credentials and CI services are available. |
| Flink ingestion | Deterministic checkpointed offset replay using the same dedupe key and commit metrics. | Add a Java/Flink connector module or external reference implementation when the public API stabilizes. |
| Proxy ingestion | Live thrift `Set/Get` pressure through `bcache2-proxy`, tenant quota, timeouts, and metrics. | Add multi-tenant auth integration and long-running soak against cloud object storage. |
| Data-node/metaserver | Production gate can combine ingestion replay with API, metrics, and raft gates. | Add broker restart/failover tests once real broker-backed workers land. |

## What The Local Test Starts

- 3 metaserver processes with Raft metadata replication.
- 3 data-node processes using shared-file storage and 3 replicas.
- 1 `bcache2-proxy` process in front of the cluster.
- Module ingest/query coverage for string, common TTL, hash, set, feature, IPS, risk, and temporal aggregate.
- Proxy thrift smoke coverage for string, hash, and feature APIs.
- Proxy ingestion pressure coverage for concurrent thrift `Set` writes and optional read verification.
- Primary and replica-eligible string and sequence benchmarks.
- Replication smoke coverage for secondary visibility.

## Recommended Command

Run this from the TemporalStore repo after Release binaries are built:

```bash
BUILD_TYPE=Release \
RESULT_DIR=/tmp/ts-ingestion-local/run \
SMOKE_DIR=/tmp/ts-ingestion-local/cluster \
CLUSTER_NAME=ingestion_local \
MS_PORT=65300 MS_RAFT_PORT=65310 MS_SNAPSHOT_PORT=65320 \
SERVER_PORT=65301 PROXY_PORT=65390 \
STRING_OPS=200 STRING_THREADS=4 \
SEQUENCE_KEYS=1 SEQUENCE_ROWS_PER_KEY=10 SEQUENCE_QUERY_OPS=10 SEQUENCE_THREADS=1 \
RUN_PROXY_INGESTION_PRESSURE=1 \
PROXY_INGESTION_PRESSURE_OPS=200 \
PROXY_INGESTION_PRESSURE_THREADS=4 \
PROXY_INGESTION_PRESSURE_VALUE_BYTES=128 \
PROXY_INGESTION_PRESSURE_VERIFY_READS=1 \
PROXY_INGESTION_PRESSURE_VERIFY_TIMEOUT_MS=10000 \
PROXY_INGESTION_PRESSURE_VERIFY_POLL_MS=20 \
PROXY_PIN_PRIMARY_READS=1 \
PROXY_EXTRA_FLAGS='--proxy_ingestion_max_write_inflight=64' \
bash tools/run_shared_file_3node_scale_ubuntu22.sh
```

Use a fresh port range when repeating the test. If startup reports a bind failure, wait for the
previous process cleanup or choose another range.

## Pass Criteria

The run is considered healthy when:

- The script exits `0` and prints `PASS shared-file 3-node scale`.
- Every `*.exit_code` is `0`.
- `module_ingest` prints `PASS ALL MODULE INGEST+QUERY TESTS`.
- `proxy_smoke` prints all proxy thrift smoke PASS lines.
- `proxy_ingestion_pressure` reports:
  - `ok` equals `ops`.
  - `write_failed=0`.
  - `rpc_failed=0`.
  - `status_failed=0`.
  - `read_failed=0` when read verification is enabled.
- `replication_smoke` reports secondary read visibility.

Example from the local validation run after the ingestion polish:

```text
proxy_ingestion_pressure
ops=200
threads=4
value_size=128
ok=200
write_failed=0
read_verified=200
verify_timeout_ms=10000
verify_poll_ms=20
rpc_failed=0
status_failed=0
read_failed=0
write_elapsed_ms=374
elapsed_ms=528
write_qps=534.759
end_to_end_qps=378.788
```

## Important Knobs

| Variable | Default | Use |
| --- | ---: | --- |
| `RUN_PROXY_INGESTION_PRESSURE` | `0` | Enables the proxy pressure client. |
| `PROXY_INGESTION_PRESSURE_OPS` | `1000` | Number of thrift `Set` writes. |
| `PROXY_INGESTION_PRESSURE_THREADS` | `4` | Concurrent pressure client threads. |
| `PROXY_INGESTION_PRESSURE_VERIFY_READS` | `0` | Verifies written keys after the write phase. |
| `PROXY_INGESTION_PRESSURE_VERIFY_TIMEOUT_MS` | `10000` | Total visibility wait used by verification. |
| `PROXY_INGESTION_PRESSURE_VERIFY_POLL_MS` | `20` | Poll interval for missing keys during verification. |
| `PROXY_PIN_PRIMARY_READS` | `1` | Routes proxy reads to primary partitions for read-after-write safety. |
| `PROXY_EXTRA_FLAGS` | empty | Extra proxy gflags such as ingestion quota limits. |

## Read Mode Guidance

Keep `PROXY_PIN_PRIMARY_READS=1` for the default public/customer ingestion path. It makes proxy
`Set` followed by proxy `Get` deterministic in local shared-store testing and avoids exposing users
to replica visibility lag.

Set `PROXY_PIN_PRIMARY_READS=0` only when explicitly testing follower/locality reads. In shared-store
mode, follower reads may lag; those tests should evaluate visibility lag separately instead of using
read-after-write pass/fail as the main ingestion gate.

## Operational Notes

- Proxy inflight quotas are write-aware: `--proxy_ingestion_max_write_inflight` limits concurrent
  write ingestion without blocking read-only requests behind the same quota.
- The pressure client measures write QPS from write completion time, then performs optional
  post-write read verification. This keeps ingestion throughput visible while still catching routing
  or visibility regressions.
- Queue-backed ingestion should reuse the proxy or ingestion-worker batching path. Kafka, Pulsar,
  Kinesis, or Pub/Sub connectors are best for replayable, bursty, or no-data-loss workloads; the
  direct proxy API remains the default low-friction path.
- Flink jobs should use checkpointed offsets and the same dedupe key so replay after task restart is
  idempotent.
- The local shared-file harness is the first gate. AWS/S3 or large cluster runs should come after
  this test is deterministic locally.
