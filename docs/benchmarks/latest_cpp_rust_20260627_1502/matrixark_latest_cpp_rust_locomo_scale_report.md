# MatrixArk Latest C++/Rust LOCOMO And Scale Report

- generated_at_utc: `2026-06-27T22:17:04.712017+00:00`
- artifact_dir: `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502`
- C++ library: `/root/src/github-services/TemporalStore/output-ubuntu22/release/sdk/lib/libbcache2.so`
- Rust runner: `/root/src/github-services/TemporalStore/target/debug/matrixark_record_log`
- model mode for benchmark slices: `hash embeddings + deterministic reader/judge`
- important: this is a functional/storage benchmark sweep, not official VikingMem-style OSS reader/judge parity.

## Summary

- Dataset files are present and validate: LOCOMO has 10 conversations / 1,986 questions / 5,882 turns; LongMemEval_s has 500 items / 500 questions / 246,750 turns.
- Rust required pipeline parity passes: ingest, extraction, async summaries, L0/L1 traversal, secondary-index filtering, events/entities/resources/skills retrieval, ContextPack, audit, and replay.
- C++ required pipeline parity still fails `required_record_types_present`; dataset ContextPack artifacts show C++ native retrieval scans records but returns zero selected refs.
- C++/Rust 1K scale comparison passes after fixing the benchmark harness cleanup bug and rebuilding Rust with `matrixark_batch_append_records`.
- Current C++ scale ingest/retrieve is faster than Rust proxy retrieve, but C++ dataset retrieval is not quality-correct until native scope/filtering returns candidates.

## Required Pipeline Parity

| Backend | OK | Error | Elapsed ms |
| --- | ---: | --- | ---: |
| cpp | False | cpp failed required_record_types_present | 939.760 |
| rust | True |  | 958.140 |

## 1K Scale Comparison

- command shape: `events=1000`, `queries=20`, `ingest-mode=batch`, `batch-size=20`, same metaserver/table/storage options.
- status: `passed`

| Metric | C++ | Rust | Delta | Rust/C++ |
| --- | ---: | ---: | ---: | ---: |
| `message_qps` | 760.147 | 624.081 | -136.066 | 0.821 |
| `ingest_p50_ms` | 24.783 | 30.486 | 5.703 | 1.230 |
| `ingest_p95_ms` | 42.547 | 47.089 | 4.542 | 1.107 |
| `ingest_p99_ms` | 48.797 | 57.274 | 8.477 | 1.174 |
| `retrieve_qps` | 11.992 | 1.615 | -10.377 | 0.135 |
| `retrieve_p50_ms` | 83.954 | 605.973 | 522.019 | 7.218 |
| `retrieve_p95_ms` | 92.978 | 693.922 | 600.944 | 7.463 |
| `retrieve_p99_ms` | 114.963 | 769.769 | 654.806 | 6.696 |
| `errors` | 0 | 0 | 0 | 1.000 |
| `timeouts` | 0 | 0 | 0 | 1.000 |

## Benchmark Slices

| Run | Questions | Turns Ingested | Ingest turns/s | Avg prompt tokens | p95 retrieve ms | Context recall | Judge score | Compression hidden |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| LOCOMO C++ direct | 20 | 788 | 578.136 | 0 | 82.151 | 0.000 | 0.000 | 0 |
| LOCOMO Rust proxy | 20 | 788 | 360.641 | 4749.700 | 611.237 | 0.950 | 0.650 | 0 |
| LongMemEval_s C++ direct | 10 | 2042 | 203.305 | 0 | 270.652 | 0.000 | 0.000 | 0 |
| LongMemEval_s Rust proxy | 10 | 2042 | 171.136 | 10000 | 1916.716 | 1.000 | 0.500 | 0 |

## C++ Native Retrieval Gap

The C++ native ContextPack path now satisfies the no-raw-record contract, but the dataset slices still return empty packs. The first C++ LOCOMO ContextPack after the scope matcher fix reports:

```json
{
  "backend": "temporalstore-direct",
  "dropped_by_scope": 1534,
  "dropped_by_type": 2537,
  "execution_mode": "cpp_direct_native_context_pack",
  "native_pack_assembly": true,
  "native_prefix_scan": true,
  "native_secondary_index_prefilter": true,
  "returned_records": 0,
  "scanned_records": 4071,
  "secondary_index_dropped_candidate_count": 0,
  "secondary_index_matched_candidate_count": 0
}
```

Interpretation: C++ storage and native scan are reachable (`scanned_records > 0`), but the C++ native scope/filter stage drops all candidate records before scoring. Rust proxy native retrieval does return selected refs under the same dataset slice. This is the next correctness gap before claiming C++ vs Rust benchmark parity.

## Fixes Applied During This Sweep

- Removed stale `temp_log` cleanup in `run_matrixark_context_storage_benchmark.py`; successful C++ runs no longer exit with `NameError` after writing a valid report.
- Rebuilt Rust `matrixark_record_log`; Rust now accepts `matrixark_batch_append_records` and the 1K scale comparison passes.
- Started aligning C++ native scope filtering with Python explicit-scope behavior, but dataset retrieval still shows candidate drops by scope, so more C++ native-filter work is needed.

## Artifacts

- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_fixed/comparison.json`
- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/scale_1k_fixed/comparison.md`
- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/locomo_cpp_after_scope_fix2/locomo_cpp_after_scope_fix2.report.json`
- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/locomo_rust/locomo_rust.report.json`
- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/longmemeval_cpp/longmemeval_cpp.report.json`
- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/longmemeval_rust/longmemeval_rust.report.json`
- `/root/src/github-services/TemporalStore/docs/benchmarks/latest_cpp_rust_20260627_1502/matrixark_required_pipeline_parity_latest_cpp_rust_20260627_1502_rerun.json`

## Next Fix

Fix C++ native ContextPack scope/filter parity so it returns the same candidate classes as Rust/Python, then rerun required pipeline parity and the LOCOMO/LongMemEval slices. After that, run OSS embedding + OSS/OpenAI-compatible reader/judge parity.
