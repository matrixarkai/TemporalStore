# C++ Context Benchmark Report Adapter

The unified benchmark cases in `compat/unified_temporalstore_cases.json` require C++ and Rust to
emit the same MatrixArk/VikingMem report shape:

```text
matrixark_vikingmem_context_benchmark_report_v1
```

Rust emits this shape from:

```bash
python3 tools/run_locomo_90_hit_rate.py
python3 tools/run_longmemeval_s_full_path.py
bash tools/run_context_benchmarks_docker_open_model.sh
```

C++ should map its native MatrixArk/VikingMem benchmark outputs through:

```text
compat/cpp_context_benchmark_report_adapter.h
```

The header is a single-file C++17 adapter template using `nlohmann::json`. It defines:

- `ContextBenchmarkThresholds`
- `ContextBenchmarkPerQueryRow`
- `ContextBenchmarkReport`
- `ToJson(...)`
- `ValidateReportContract(...)`

## Required C++ Mapping

| Shared field | C++ source meaning |
| --- | --- |
| `benchmark_family` | Always `vikingmem_long_memory`. |
| `benchmark_hit_at_k` / `hit_rate` | Retrieval/context Hit@K. |
| `benchmark_recall_at_k` | Same as Hit@K unless C++ reports a distinct recall value. |
| `benchmark_mean_reciprocal_rank` | MRR over all benchmark questions. |
| `benchmark_token_reduction_percent` | Percent reduction from full source tokens to retrieved context tokens. |
| `benchmark_retrieval_p50_ms`, `benchmark_retrieval_p95_ms` | Retrieval latency percentiles. |
| `benchmark_reader_p50_ms`, `benchmark_reader_p95_ms` | Reader/answer generation latency percentiles. |
| `reader_hit_rate` | Answer/reader hit rate. |
| `reader_mode_requested`, `reader_mode_effective` | `deterministic`, `open-source`, or equivalent C++ mode labels. |
| `reader_provider_name` | Provider profile, for example `matrixark-cpp-oss-context`. |
| `reader_model` | Reader model identifier. |
| `benchmark_thresholds` | The selected shared threshold profile values. |
| `benchmark_threshold_violations` | Exact threshold failure strings. |
| `benchmark_per_query` | One row per scored query, keyed by `query_id`. |

Each `benchmark_per_query` row must include:

```text
query_id
category
hit
rank
reader_hit
retrieval_ms
reader_ms
retrieved_blocks
retrieved_tokens
source_tokens
token_reduction_percent
```

## Archive Layout

Both repos should archive benchmark outputs as:

```text
<archive>/
  manifest.json
  <dataset>_report.json
  <dataset>_misses.jsonl
```

Missing real datasets must be reported as skipped or blocked in `manifest.json`, never as a passing
fixture score.

## Cross-Repo Comparison

Compare C++ and Rust reports with:

```bash
python3 tools/compare_context_benchmark_reports.py \
  --rust-report /tmp/rust_locomo_report.json \
  --cpp-report /tmp/cpp_locomo_report.json \
  --case-name context_benchmark_full_dataset_gates \
  --dataset locomo
```

The comparator checks:

- all required report fields from the shared corpus
- threshold object keys and values
- summary fields: Hit@K, recall, MRR, token reduction, reader hit rate, threshold violations
- per-query `query_id` coverage
- per-query hit, reader-hit, rank, category, token counts, and token reduction
- latency fields are present, non-negative, and within a configurable ratio

Use `--numeric-tolerance` for floating-point drift and `--latency-ratio-tolerance` for expected
runtime differences between C++ and Rust environments.
