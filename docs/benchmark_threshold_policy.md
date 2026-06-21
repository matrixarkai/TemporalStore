# Context Benchmark Threshold Policy

This policy separates smoke-fixture gates from full-dataset production gates.
Fixture gates prove that ingestion, extraction, retrieval, reader scoring, and
report emission are wired correctly. Full-dataset gates are the only thresholds
that can support production readiness claims.

## Claim Rules

Benchmark result docs must stay strict:

- Fixture scores are wiring evidence only; they must not be called paper-equivalent scores or
  production-parity evidence.
- Deterministic-reader full-dataset scores can claim retrieval/reader gate readiness for that
  configured runner, but not live OSS-reader parity.
- OSS/open-model claims require at least one successful real local reader call and
  `require_open_source_reader=true`.
- Production parity, production-ready, or paper-comparable benchmark claims require all evidence in
  the same result doc: real dataset artifact, live OSS reader calls, full Rust TemporalStore replay,
  all-pipeline Rust evidence, and passing threshold output with zero threshold violations.
- The archive gate must also show `reader_mode_effective=open-source`,
  `require_open_source_reader=true`, `python_only_diagnostic=false`, and a non-fixture full-dataset
  case threshold.
- Paper-comparable archives must include dataset hash, model/provider, reader mode, prompt,
  thresholds, p50/p95 latencies, token reduction, and category breakdown.
- Missing artifacts, skipped datasets, Docker/model pull failures, fallback readers, and fixture
  gates must be recorded as blocked, skipped, or fixture-only evidence.

`tools/validate_benchmark_claims.py` enforces this wording guard for benchmark result docs.

## Profiles

| Profile | Intended use | Min cases | Min retrieval hit | Min reader hit | Min token reduction | Retrieval p95 | Reader p95 | OSS reader |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `fixture` | Checked-in fixtures and CI smoke runs | 4 | 1.00 | 1.00 | 0% | 1000 ms | 30000 ms | No |
| `locomo_full` | Real LOCOMO 10-conversation benchmark artifact | 1542 | 0.94 | 0.58 | 80% | 250 ms | 50 ms | No |
| `longmemeval_full` | Real LongMemEval_s mounted artifact | 500 | 0.90 | 0.58 | 80% | 2000 ms | 200 ms | No |
| `oss_reader_full` | Full LOCOMO with a live local OpenAI-compatible OSS reader | 1542 | 0.94 | 0.58 | 80% | 250 ms | 30000 ms | Yes |

## Rationale

- `fixture` keeps CI deterministic and fast. It intentionally does not require
  token reduction because small fixture conversations can fit entirely inside the
  retrieval window.
- `locomo_full` is based on the current real LOCOMO artifact with 1542 scored
  cases. The 0.94 retrieval threshold preserves the current full-conversation
  retrieval gate and prevents reader improvements from hiding retrieval regressions, while
  0.58 reader hit rate preserves the current deterministic reader improvement as
  a hard floor instead of silently accepting reader regressions.
- `locomo_full` and `oss_reader_full` reject `--evidence-window` because that
  option uses gold evidence references. Evidence windows are diagnostic-only and
  must not satisfy production scoring.
- `longmemeval_full` stays separate because the real artifact is much larger and
  has different latency characteristics. The committed fixture must not be used
  to satisfy this gate.
- `oss_reader_full` requires at least one successful local open-source reader
  call. Its reader p95 budget is wider because a local model gateway is expected
  to be slower than the deterministic extractor.

## Commands

LOCOMO production gate:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --threshold-profile locomo_full \
  --input /tmp/locomo10.json \
  --report /tmp/temporalstore_locomo_threshold_policy_result.json \
  --misses /tmp/temporalstore_locomo_threshold_policy_misses.jsonl
```

LongMemEval_s fixture smoke gate:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile fixture \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --reader-mode auto \
  --report /tmp/temporalstore_longmemeval_fixture_threshold_policy_result.json \
  --misses /tmp/temporalstore_longmemeval_fixture_threshold_policy_misses.jsonl
```

LongMemEval_s production gate, once the real artifact is mounted:

```bash
python3 tools/fetch_longmemeval_s.py --output /tmp/longmemeval_s.json
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile longmemeval_full \
  --input /tmp/longmemeval_s.json \
  --reader-mode deterministic \
  --report /tmp/temporalstore_longmemeval_full_threshold_policy_result.json \
  --misses /tmp/temporalstore_longmemeval_full_threshold_policy_misses.jsonl
```

LOCOMO and LongMemEval_s wrappers require the Rust TemporalStore backend by default. That path
converts benchmark cases to the Rust context JSONL contract and runs
`context_workflow_harness` through a real `TemporalEngine` before the deterministic Python
reader/scorer emits the final benchmark report. The runner also scores that exact converted subset
with the Python ranker and requires Rust case count, Hit@K, mean reciprocal rank, and zero-hit
query count to match the Python scorer within `--rust-temporalstore-score-tolerance` before marking
the backend ready. Reports also emit `all_pipelines_use_rust_temporalstore`; production benchmark
evidence requires that field to be `true`. A fixed `1e-6` numerical epsilon is always allowed so
Rust f32 report output does not fail exact source-rank parity. Use `--skip-rust-temporalstore`
only with `--allow-python-only-diagnostic` for explicit diagnostic Python-only runs; those reports
are not Rust-backed evidence. Use
`--rust-temporalstore-max-cases 0 --rust-temporalstore-source-limit 0` only when a full Rust
backend replay is intentionally requested; the default bounded proof keeps local full-dataset
scoring practical while preventing Python-only benchmark evidence. Full replay is batched by
default when `--require-full-rust-temporalstore-replay` is set. Use
`--rust-temporalstore-batch-size <N>` to tune the batch size; timeout reports include the failed
batch index, batch path, completed batches, and stdout/stderr tails. Add
`--rust-temporalstore-release` for production/full-dataset archives; the default dev build is kept
for fast local bounded proof.

The standalone Rust `context_workflow_harness` built-in external fixture gates pipeline execution
on complete Hit@K coverage, no zero-hit queries, no missing answer terms, and no missing expected
refs. It still reports MRR/category MRR for quality tracking, but rank-1 ordering is not required
for the built-in pipeline readiness assertion because Rust retrieval can emit valid internal
context blocks before the exact source block.

For production benchmark claims, prefer the explicit guard:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --threshold-profile locomo_full \
  --input /tmp/locomo10.json \
  --require-full-rust-temporalstore-replay \
  --rust-temporalstore-batch-size 16 \
  --rust-temporalstore-release
```

The same flag is supported by `tools/run_longmemeval_s_full_path.py`. It forces all converted cases
and all source records through the Rust `context_workflow_harness`, emits
`rust_temporalstore_full_replay_ready`, records `batch_replay_used` and `batch_reports`, and adds
`full_rust_temporalstore_replay_not_ready` to the threshold violations if the all-case Rust
evidence is incomplete.

For Docker/open-model archives, set:

```bash
TEMPORALSTORE_REQUIRE_FULL_RUST_REPLAY=1
TEMPORALSTORE_RUST_BACKEND_RELEASE=1
```

This keeps the fast default for iteration while making paper-comparable archives fail closed unless
the Rust backend evidence is all-case and untrimmed.

Explicit threshold flags override profile defaults. Use overrides only when the
artifact contract changes, and record the reason in the reproducibility evidence
doc with the command, input hash, case count, model mode, reader mode, thresholds,
and report path.

The Docker/open-model runner for `oss_reader_full` is documented in
[context_benchmarks_docker_open_model.md](context_benchmarks_docker_open_model.md).
Use `tools/run_context_benchmarks_oss_reader_endpoint.sh` when validating the exact
C++/MatrixArk/OpenViking reader profile against an existing OpenAI-compatible
endpoint. That path defaults to `matrixark-cpp-oss-context` and
`google/flan-t5-small`, and must report `reader_open_source_calls > 0`.
