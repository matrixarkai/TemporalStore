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

Archived raw comparison details:

| Artifact | Rust write QPS vs C++ | Rust write p95 vs C++ | Rust read QPS vs C++ | Rust read p95 vs C++ |
|---|---:|---:|---:|---:|
| `context40_fixed` | +21.862% | +197.704% | -51.144% | +38.265% |
| `raw100_fixed` | -1.492% | +288.950% | -51.456% | +22.535% |
| `raw1k_fixed` | +9.339% | +264.614% | -57.111% | -27.546% |

Latest local Rust functional scale artifact:

- `/tmp/temporalstore-rust-scale-parity-1782962380/scale_harness.json`

This was a debug-profile local Rust scale run after a release build timed out during compilation, so it is
functional scale evidence, not a release performance parity claim.

| Metric | Value |
|---|---:|
| string ops | 60 |
| hash ops | 15 |
| sequence rows | 160 |
| sampled reads | 10 |
| failovers | 4 |
| scale events | 3 |
| elapsed ms | 35604 |
| write ops/sec | 2.162678 |
| replication healthy | true |
| max replica lag | 0 |
| Raft write p50/p95/p99 us | 102049 / 133094 / 167212 |
| Raft replica read p50/p95/p99 us | 8158 / 104994 / 104994 |
| sync primary write QPS | 10.718 |
| async primary write QPS | 10.816 |
| sync storage write p50/p95/p99 us | 692 / 1126 / 1544 |
| async enqueue p50/p95/p99 us | 7 / 11 / 50 |
| async flush p50/p95 us | 10983 / 23446 |
| sync replica read QPS | 10.718 |
| async replica read QPS | 0.541 |
| sync / async max lag | 0 / 19 |

The run reported the Rust deployment path healthy for Docker/AWS SLO evidence, storage deployment scale SLO,
metaserver/proxy/client/data-node process evidence, Raft failover, storage/cache pressure, proxy convergence,
and workload replay. CPU, memory, disk, and network collectors were still pending in that local artifact.

## Runner Changes

`tools/run_matrixark_cpp_rust_scale_report.py` now includes:

- batched raw read measurement through `batch_hget` when the backend supports it;
- a performance parity gate in `comparison.json`;
- `--perf-min-qps-ratio` for QPS metrics, default `0.8`;
- `--perf-max-latency-ratio` for p50/p95/p99 metrics, default `2.0`;
- `--require-perf-parity` to fail the run when Rust falls outside the configured budget.

`MatrixArkRustCliClient` now keeps one serialized write/control process and a bounded read-process pool for
`batch_hget`. The pool is controlled by `MATRIXARK_RUST_GATEWAY_READ_POOL_SIZE` and defaults to `4`.
This targets the archived raw read QPS gap without making unsafe writes concurrent in the local direct-SDK bridge.
`metrics_snapshot()` exposes `read_pool_size`, `read_pool_enabled`, and `max_inflight` so scale reports can verify
which bridge path was used.

The default report still writes comparison artifacts when parity fails. CI or release validation should pass
`--require-perf-parity` for the scale gate.

## Remaining Gaps

- Raw storage write tail latency needs more tuning in the Rust direct SDK bridge and storage write path.
- Raw read throughput needs a fresh release-mode C++/Rust comparison after the Rust read-pool bridge change.
- Full C++ parity requires a fresh local scale run with the same raw/context workload, same batch sizes,
  same worker counts, and `--require-perf-parity`.
- Release-mode Rust scale testing should be repeated after the release binary is already built, because the
  latest release attempt timed out during compilation rather than during the scale workload.

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
