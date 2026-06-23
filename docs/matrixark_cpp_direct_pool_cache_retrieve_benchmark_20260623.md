# MatrixArk C++ Direct Retrieval Pool And Cache Benchmark

Date: 2026-06-23

## What Changed

- `MatrixArkTemporalStoreDirectAdapter` now has an adapter-local record lock.
- `read_all()` now uses a process-wide per-prefix singleflight load lock, keyed by the `record_count` watermark cache.
- The C++ direct scale runner now uses a shared prefix service/adapter pool instead of constructing a fresh C++ SDK adapter per retrieval request.
- The runner can pre-warm the pool before measured retrieval, so each prefix has its records loaded once before concurrent retrieval starts.

## Validation

- `python3 -m py_compile tools/matrixark_mcp_server.py tools/run_matrixark_cpp_direct_scale_benchmark.py tools/test_matrixark_direct_adapter_compact_log.py`
- `python3 tools/test_matrixark_direct_adapter_compact_log.py`
- Local C++ TemporalStore service was restarted with `tools/deploy_local_ubuntu22.sh stop/start/status` before the successful run.

## Successful 4 / 8 / 16 Retrieve Worker Run

This run used 4 ingested prefixes, 20-message batch extraction per prefix, and 16 retrieval operations per worker-level. The pool was warmed before measured retrieval.

| Retrieve workers | Status | Retrieve QPS | p50 ms | p95 ms | p99 ms | Max ms | Pool size | Pool warmup ms | Errors |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 4 | passed | 0.256 | 60.838 | 2193.476 | 60130.624 | 60130.624 | 4 | 10.403 | 0 |
| 8 | passed | 1.304 | 6964.246 | 12244.693 | 12252.707 | 12252.707 | 4 | 17.508 | 0 |
| 16 | passed | 0.869 | 13269.201 | 13377.031 | 18375.659 | 18375.659 | 4 | 19.424 | 0 |

## Full 12-Prefix Attempt

A full 12-ingest / 24-retrieve warm run was attempted first. The 4-worker and 8-worker reports completed, but the 16-worker run exceeded the outer command timeout. A later warm run after service restart exposed C++ service instability during the ingest phase before retrieval measurement. Those artifacts are kept as diagnostic JSON, but they are not used as the clean comparison table above.

## Interpretation

- The requested adapter/client reuse is now implemented as a shared prefix pool. The successful run created exactly one service per storage prefix.
- The requested `read_all()` prefix/watermark cache is now protected by a singleflight load lock. A focused unit test verifies that a second adapter for the same prefix does not reload all hash records when the watermark has not changed.
- The remaining high-concurrency tail is not explained by cold `read_all()` hydration: pool warmup completed in about 10-20 ms and had zero errors.
- Retrieval still writes `ContextPackAudit` records, so the benchmark is not read-only. The long tail is now most likely C++ direct write/audit contention plus OSS model/scoring contention under high Python concurrency.

## Next Bottlenecks

1. Add async or buffered ContextPackAudit writes for retrieval benchmarks and production serving.
2. Push ContextIndex/tree traversal into native C++ APIs so Python does not repeatedly score full record sets.
3. Add a shared/batched model worker for query understanding and embeddings.
4. Keep retrieval hard deadlines enabled in production paths so a slow audit/write cannot block the returned ContextPack.

## Artifact Files

- `docs/matrixark_cpp_direct_scale_benchmark_pool_cache_warm_small_retrieve_4_20260623.json`
- `docs/matrixark_cpp_direct_scale_benchmark_pool_cache_warm_small_retrieve_8_20260623.json`
- `docs/matrixark_cpp_direct_scale_benchmark_pool_cache_warm_small_retrieve_16_20260623.json`
