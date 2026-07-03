# C++ Vs Rust Context Scale Parity - 2026-07-02

## Scope

This run exercised the shared MatrixArk C++/Rust scale harness for:

- raw storage write/read QPS and latency
- Rust-backed context ingestion
- Rust-backed context retrieval latency
- end-to-end context selected-reference correctness gating

Python was used only as the harness/orchestration layer. Rust context runs used the Rust TemporalStore CLI with `LD_LIBRARY_PATH` pointed at the local C++ SDK library directory required by the current binary.

## Commands

Full context comparison with C++ and Rust:

```bash
LD_LIBRARY_PATH=<repo>/output-ubuntu22/release/sdk/lib \
python3 tools/run_matrixark_cpp_rust_scale_report.py \
  --events 200 \
  --raw-ops 200 \
  --raw-read-ops 100 \
  --raw-workers 4 \
  --messages-per-ingest 10 \
  --ingest-workers 4 \
  --retrieve-queries 64 \
  --retrieve-workers 8 \
  --max-context-tokens 12000 \
  --backend-worker-timeout-sec 900 \
  --request-timeout-ms 60000 \
  --io-timeout-ms 60000 \
  --ingest-deadline-ms 60000 \
  --retrieve-deadline-ms 10000 \
  --cpp-lib <repo>/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --rust-cli <repo>/sdk/rust/temporalstore/target/release/matrixark_record_log \
  --artifact-dir docs/benchmarks/current_cpp_rust_context_scale_20260702_ldpath \
  --run-id current_cpp_rust_context_scale_ldpath \
  --require-perf-parity
```

Rust-only context diagnostic:

```bash
LD_LIBRARY_PATH=<repo>/output-ubuntu22/release/sdk/lib \
python3 tools/run_matrixark_cpp_rust_scale_report.py \
  --backends rust \
  --events 50 \
  --raw-ops 50 \
  --raw-read-ops 25 \
  --raw-workers 1 \
  --messages-per-ingest 5 \
  --ingest-workers 1 \
  --retrieve-queries 16 \
  --retrieve-workers 1 \
  --max-context-tokens 12000 \
  --backend-worker-timeout-sec 600 \
  --request-timeout-ms 60000 \
  --io-timeout-ms 60000 \
  --ingest-deadline-ms 60000 \
  --retrieve-deadline-ms 10000 \
  --cpp-lib <repo>/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --rust-cli <repo>/sdk/rust/temporalstore/target/release/matrixark_record_log \
  --artifact-dir docs/benchmarks/current_rust_context_scale_diag_20260702 \
  --run-id current_rust_context_scale_diag
```

Raw storage C++/Rust comparison:

```bash
LD_LIBRARY_PATH=<repo>/output-ubuntu22/release/sdk/lib \
python3 tools/run_matrixark_cpp_rust_scale_report.py \
  --skip-context-pipeline \
  --events 10 \
  --raw-ops 100 \
  --raw-read-ops 50 \
  --raw-workers 2 \
  --messages-per-ingest 5 \
  --ingest-workers 1 \
  --retrieve-queries 0 \
  --retrieve-workers 1 \
  --backend-worker-timeout-sec 300 \
  --request-timeout-ms 60000 \
  --io-timeout-ms 60000 \
  --cpp-lib <repo>/output-ubuntu22/release/sdk/lib/libbcache2.so \
  --rust-cli <repo>/sdk/rust/temporalstore/target/release/matrixark_record_log \
  --artifact-dir docs/benchmarks/current_cpp_rust_raw_compare_20260702 \
  --run-id current_cpp_rust_raw_compare
```

## Results

### Raw Storage

Raw storage parity passed on the same small workload.

| Metric | C++ | Rust | Result |
|---|---:|---:|---|
| write record QPS | 168.418 | 200.721 | Rust 1.19x C++ |
| write p95 | 591.936 ms | 495.080 ms | Rust 0.84x C++ |
| read QPS | 1315.825 | 1885.554 | Rust 1.43x C++ |
| read p95 | 36.403 ms | 25.119 ms | Rust 0.69x C++ |

Artifact: `docs/benchmarks/current_cpp_rust_raw_compare_20260702/comparison.md`.

### Rust Context Pipeline Diagnostic

Rust context ingestion and retrieval completed without errors in the smaller single-worker diagnostic.

| Metric | Rust |
|---|---:|
| message QPS | 2.623 |
| ingest ops | 10 |
| ingest p50 | 1007.747 ms |
| ingest p95 | 4063.191 ms |
| retrieve QPS | 315.789 |
| retrieve p50 | 1.875 ms |
| retrieve p95 | 4.947 ms |
| retrieve p99 | 18.354 ms |
| selected refs avg/max | 0.0 / 0 |

Artifact: `docs/benchmarks/current_rust_context_scale_diag_20260702/comparison.md`.

### Full Context Comparison

The full C++/Rust context comparison is not production parity evidence yet.

- C++ context worker exited with return code `-11` before writing a result artifact.
- Rust backend was ready after setting `LD_LIBRARY_PATH`, but the full run still had 4 ingest deadline errors and retrieval returned `selected_refs_max=0`.
- Rust full-run metrics before the correctness gate failed:
  - message QPS: `2.361`
  - ingest p50: `5563.046 ms`
  - ingest p95: `36047.739 ms`
  - retrieve QPS: `403.749`
  - retrieve p50: `16.834 ms`
  - retrieve p95: `32.478 ms`

Artifact: `docs/benchmarks/current_cpp_rust_context_scale_20260702_ldpath/comparison.md`.

## Current Parity Posture

Raw storage QPS and latency are healthy on this small local run. End-to-end context ingestion/extraction/retrieval cannot claim C++/Rust parity yet because correctness failed before latency can be evaluated.

The immediate blockers are:

1. C++ context worker crash in the full context comparison.
2. Rust scale retrieval selected zero refs despite successful small-run ingestion.
3. Rust full context run hit ingest deadline errors under 4 ingest workers.
4. Phase 1 native retrieval correctness evidence remains missing for scope filtering, placement filtering, compact secondary index prefiltering, stale superseded exclusion, shared resource/skill quota, and cross-session quota rerank.

## Next Fix Order

1. Fix Rust selected-ref production in the scale harness path.
2. Re-run Rust-only context scale with selected refs non-empty.
3. Debug the C++ context worker return code `-11` separately from raw storage.
4. Re-run C++/Rust full context comparison with both backends passing.
5. Only then evaluate QPS and latency parity for end-to-end context pipelines.
