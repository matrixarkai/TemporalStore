# Rust/C++ Ingestion And Retrieval Perf Parity

This page records the current scale-testing contract for Rust TemporalStore versus C++ TemporalStore.
It is intentionally evidence-oriented: context pipeline parity, raw storage parity, and remaining blockers
are tracked separately.

## Current Evidence

Latest archived comparison artifacts:

- `docs/benchmarks/cpp_rust_scale_20260629_context40_fixed/comparison.json`
- `docs/benchmarks/cpp_rust_scale_20260629_raw100_fixed/comparison.json`
- `docs/benchmarks/cpp_rust_scale_20260629_raw1k_fixed/comparison.json`

Observed shape:

| Area | Status |
|---|---|
| Context ingestion | Rust is faster than C++ in the archived context40 run. |
| Context retrieval | Rust is faster than C++ in the archived context40 run. |
| Raw write throughput | Rust is near or above C++ record QPS in raw100/raw1k. |
| Raw write p95 | Rust is still worse than C++ in raw100/raw1k. |
| Raw read QPS | Rust is still below C++ in raw100/raw1k. |
| Raw read p95 | Rust is competitive or better in raw1k, slightly worse in raw100. |

## Runner Changes

`tools/run_matrixark_cpp_rust_scale_report.py` now includes:

- batched raw read measurement through `batch_hget` when the backend supports it;
- a performance parity gate in `comparison.json`;
- `--perf-min-qps-ratio` for QPS metrics, default `0.8`;
- `--perf-max-latency-ratio` for p50/p95/p99 metrics, default `2.0`;
- `--require-perf-parity` to fail the run when Rust falls outside the configured budget.

The default report still writes comparison artifacts when parity fails. CI or release validation should pass
`--require-perf-parity` for the scale gate.

## Remaining Gaps

- Raw storage write tail latency needs more tuning in the Rust direct SDK bridge and storage write path.
- Raw read throughput needs scale validation after batched read measurement.
- Full C++ parity requires a fresh local scale run with the same raw/context workload, same batch sizes,
  same worker counts, and `--require-perf-parity`.

## Required Scale Command

Example strict gate:

```bash
python3 tools/run_matrixark_cpp_rust_scale_report.py \
  --events 1000 \
  --raw-ops 1000 \
  --raw-read-ops 500 \
  --raw-batch-size 50 \
  --raw-read-batch-size 25 \
  --require-perf-parity
```

The report is parity-ready only when `comparison.perf_parity.passed` is `true`.
