# MatrixArk C++ TemporalStore Scale Benchmark Summary - 2026-06-23

## Purpose

Run MatrixArk extraction, ingestion, async summary refresh, and retrieval through
real C++ TemporalStore with OSS models required. This is a product-pipeline
scale test, not a raw C++ engine microbenchmark.

## Test Setup

- Backend: `temporalstore-direct`
- C++ metaserver: `127.0.0.1:18000`
- Namespace/table: `deploy_ns/deploy_table`
- SDK: `output-ubuntu22/release/sdk/lib/libbcache2.so`
- Embedding model: `.local/context-oss-models/sentence-transformers/all-MiniLM-L6-v2`
- Understanding provider: `oss_encoder`
- Deterministic extraction/query fallback: disabled
- Batch size: 20 messages per extraction batch

Each ingest operation runs:

```text
20-message logical batch
-> OSS encoder extraction/query labels
-> ContextEvent writes
-> ContextEntity writes
-> ContextSegment writes
-> ContextIndex writes
-> ContextSummary and ContextEmbedding writes
-> async summary refresh
-> C++ TemporalStore persistence
```

Each retrieval operation runs:

```text
new direct C++ adapter
-> read persisted MatrixArk records from C++ TemporalStore
-> query understanding with OSS encoder
-> tree/summary/index/event/entity retrieval
-> ContextPack construction
```

## Results

### Recommended Local Cap From This Run

Use this cap for current local debugging:

- Ingest: 2 concurrent batch extract workers
- Retrieval: 4 concurrent retrieval workers
- Batch size: 20 messages
- No fallback to deterministic extraction/query understanding

At this cap:

- Ingest operations: 8
- Ingest errors: 0
- Ingest QPS: 0.243 batch ops/sec
- Message ingest QPS: 4.852 messages/sec
- Ingest p95: 8.965 sec per 20-message batch
- Retrieve operations: 24
- Retrieve errors: 0
- Retrieve QPS: 3.737 ops/sec
- Retrieve p50: 602.859 ms
- Retrieve p95: 3.395 sec
- Retrieve p99: 3.520 sec

Artifact:

- `docs/matrixark_cpp_direct_scale_benchmark_warm.html`
- `docs/matrixark_cpp_direct_scale_benchmark_warm.json`

### Higher-Concurrency Probe

The higher-concurrency probe also completed with zero errors, but latency tails
were not acceptable for serving:

- Ingest concurrency: 4
- Retrieve concurrency: 8
- Ingest errors: 0
- Retrieve errors: 0
- Ingest QPS: 0.245 batch ops/sec
- Message ingest QPS: 4.91 messages/sec
- Ingest p95: 17.028 sec
- Retrieve QPS: 0.327 ops/sec
- Retrieve p50: 1.893 sec
- Retrieve p95: 61.610 sec
- Retrieve p99: 61.646 sec

Artifact:

- `docs/matrixark_cpp_direct_scale_benchmark_cap.html`
- `docs/matrixark_cpp_direct_scale_benchmark_cap.json`

## Interpretation

The current safe local cap is 2 ingest workers and 4 retrieve workers. Increasing
retrieval to 8 workers causes a severe long-tail latency spike even though there
are no request errors. That means the next optimization target is not correctness;
it is serving-time concurrency and tail-latency control.

The likely bottlenecks are:

- Python orchestration and per-request record reload from C++ TemporalStore.
- OSS sentence-transformer encoding under concurrent request load.
- Retrieval reading all records under each prefix before scoring.
- C++ direct SDK request serialization and timeout behavior under many concurrent
  read adapters.

## Required Next Caps

Until the next optimization pass, use these limits for local C++ debug runs:

- `MATRIXARK_MAX_INGEST_WORKERS=2`
- `MATRIXARK_MAX_RETRIEVE_WORKERS=4`
- `MATRIXARK_BATCH_SIZE=20`
- Keep `MATRIXARK_REQUIRE_OSS_EMBEDDINGS=1`
- Keep `MATRIXARK_REQUIRE_OSS_UNDERSTANDING=1`

## Next Optimizations

1. Reuse a process-local service/adapter pool for retrieval instead of creating a
   fresh adapter per request.
2. Cache loaded C++ records by prefix with an invalidation watermark.
3. Push more prefix/index scans into native C++ context APIs.
4. Add a shared model worker or batched embedding encode path.
5. Add hard retrieval deadlines with partial ContextPack fallback.
6. Add raw C++ engine microbenchmarks separately from MatrixArk product-pipeline
   benchmarks.

