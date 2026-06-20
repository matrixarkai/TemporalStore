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
| `category_breakdown` | Per reasoning-category case count, Hit@K, MRR, reader hit rate, answer coverage, and zero-hit count. |
| `weak_categories` | Categories below the shared threshold policy, each with explicit reasons. |
| `weak_category_policy` | The category-level threshold values used to classify weak categories. |
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
  <dataset>_paper_comparable_report.json
```

Missing real datasets must be reported as skipped or blocked in `manifest.json`, never as a passing
fixture score.

## Cross-Repo Comparison

Execution order for benchmark parity is strict:

1. benchmark truth first: prove whether the archive is fixture/contract evidence or production
   evidence, and report skipped real datasets as blockers.
2. unified report contract next: validate both sides emit
   `matrixark_vikingmem_context_benchmark_report_v1` with the same summary, threshold,
   category, and per-query fields.
3. deeper C++ parity execution last: once C++ has a native benchmark runner, compare its archive
   against the Rust archive instead of relying on static path checks.

Compare C++ and Rust reports with:

```bash
python3 tools/compare_context_benchmark_reports.py \
  --rust-report /tmp/rust_locomo_report.json \
  --cpp-report /tmp/cpp_locomo_report.json \
  --case-name context_benchmark_full_dataset_gates \
  --dataset locomo
```

Compare complete Docker/open-model archive directories with:

```bash
python3 tools/compare_context_benchmark_archives.py \
  --rust-archive /tmp/rust_context_benchmark_archive \
  --cpp-archive /tmp/cpp_context_benchmark_archive \
  --case-name context_benchmark_full_dataset_gates \
  --datasets locomo longmemeval_s \
  --truth-mode production
```

Use `--truth-mode production` or `--require-executed` only for a real full-dataset evidence pass.
Without it, the archive comparator accepts explicit skipped/not-run statuses on both sides, which
keeps missing LOCOMO, LongMemEval_s, or live OSS-reader artifacts honest instead of turning fixture
or skipped runs into production evidence. The archive result exposes `benchmark_truth_ready` and
`truth_blockers` so readiness can separate truthful contract evidence from production evidence.

The comparator checks:

- all required report fields from the shared corpus
- threshold object keys and values
- per-category Hit@K, MRR, reader fields, answer coverage, and weak-category diagnostics
- summary fields: Hit@K, recall, MRR, token reduction, reader hit rate, threshold violations
- per-query `query_id` coverage
- per-query hit, reader-hit, rank, category, token counts, and token reduction
- latency fields are present, non-negative, and within a configurable ratio

The report comparator also emits a case-by-case miss taxonomy:

- `rust_only`: C++ hit the query but Rust missed it.
- `cpp_only`: Rust hit the query but C++ missed it.
- `shared_hard`: both Rust and C++ missed the same query.

The same taxonomy is emitted for `reader_hit` as `reader_rust_only`,
`reader_cpp_only`, and `reader_shared_hard`. Shared-hard misses are tracked as benchmark
quality gaps, while Rust-only and C++-only misses are parity deltas because the two systems
disagree on the same query.

The archive comparator checks:

- `manifest.json` exists in both archives
- reader model and optional reader endpoint metadata match
- each requested dataset has matching C++/Rust execution status
- skipped real datasets remain explicit unless `--require-executed` is set
- passed datasets contain report JSON files and pass the per-report comparator
- archive-level miss totals across all compared datasets

Use `--numeric-tolerance` for floating-point drift and `--latency-ratio-tolerance` for expected
runtime differences between C++ and Rust environments.
