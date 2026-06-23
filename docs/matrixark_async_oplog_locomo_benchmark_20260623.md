# MatrixArk Async Oplog LOCOMO Benchmark - 2026-06-23

## Result

Using C++ TemporalStore with async storage enabled, the previously stressful all-conversation LOCOMO shape completed locally.

## Runtime

TemporalStore local deployment:

```text
BUILD_TYPE=Release
TEMPORALSTORE_STORAGE_ASYNC=true
TEMPORALSTORE_STORAGE_OPLOG_DELAY_DUMP_LENGTH=10000
SERVER_COUNT=1
REPLICA_COUNT=1
```

Observed server command included:

```text
--storage_async=true
--storage_oplog_delay_dump_length=10000
```

MatrixArk benchmark settings:

```text
MATRIXARK_DIRECT_AUDIT_MODE=deferred
MATRIXARK_DIRECT_WRITE_RETRIES=5
MATRIXARK_DIRECT_WRITE_BACKOFF_MS=50
backend=temporalstore-direct
dataset=locomo10.json
batch_size=40
question_limit=100
request_timeout_ms=120000
io_timeout_ms=120000
```

## Metrics

| Metric | Value |
| --- | ---: |
| conversations | 10 |
| sessions | 272 |
| turns ingested | 5882 |
| questions run | 100 |
| ingestion elapsed ms | 8276 |
| ingestion throughput turns/sec | 710.73 |
| avg retrieval latency ms | 97.86 |
| p50 retrieval latency ms | 98.00 |
| p95 retrieval latency ms | 117.87 |
| context recall | 1.0000 |
| final debug judge score | 0.6200 |
| answer support hit | 0.6200 |
| answer hit | 0.1000 |
| evidence session recall | 0.4000 |
| avg prompt tokens | 1199.1 |
| compression hidden answers | 0 |

Artifacts were written under:

```text
/tmp/matrixark-async-oplog-locomo-20260623_154435/
```

## Interpretation

The prior all-conversation timeout was primarily caused by sync storage pressure on the local single-node deployment, plus retrieval audit pressure. With async storage and deferred audits, the same all-conversation / 100-question LOCOMO shape completed cleanly.

This does not mean all production durability concerns disappear:

- async storage returns before every oplog flush is fully committed;
- benchmark throughput is now closer to the intended async serving mode;
- strict durability benchmarks should still run with `TEMPORALSTORE_STORAGE_ASYNC=false`;
- production should expose both modes by policy: sync for correctness-sensitive writes, async for high-throughput context ingestion where replay/checkpointing is acceptable.

## Remaining Work

1. Repeat the same shape with Rust TemporalStore once the Rust long-lived gateway is configured for equivalent async behavior.
2. Run full question count, not only 100 questions.
3. Add a benchmark matrix: sync vs async storage, buffered vs deferred audit, one data node vs multiple data nodes.
4. Add native atomic batch append so `record_bundle` and `record_count` advance in one server-side operation.

