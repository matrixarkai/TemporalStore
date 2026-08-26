# Context Benchmark Threshold Policy

This policy separates smoke-fixture gates from full-dataset production gates.
Fixture gates prove that ingestion, extraction, retrieval, reader scoring, and
report emission are wired correctly. Full-dataset gates are the only thresholds
that can support production readiness claims.

## Claim Rules

Benchmark result docs must stay strict:

- Fixture scores are wiring evidence only; they must not be called paper-equivalent scores or
  production-conformance evidence.
- Deterministic-reader full-dataset scores can claim retrieval/reader gate readiness for that
  configured runner, but not live OSS-reader conformance.
- OSS/open-model claims require at least one successful real local reader call and
  `require_open_source_reader=true`.
- MatrixArk vs the external OSS baseline OSS comparisons must pass the shared OSS model contract:
  same reader model, same embedding/encoding model, same retrieved-event budget, same reader
  context budget, same reader prompt policy, and declared provider identities for both sides.
  Mismatched model, budget, or reader-policy runs are diagnostic-only even when their individual
  scores look good.
- Production conformance, production-ready, or paper-comparable benchmark claims require all evidence in
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
`tools/validate_oss_model_contract.py` enforces the shared OSS model contract across MatrixArk and
the external OSS baseline JSON artifacts before token-savings or reader-quality claims are comparable.
It fails closed by default on diagnostic-only rows, reader fallback/errors, and prompt-policy drift;
exceptions must be explicitly marked with the diagnostic flags.

The benchmark runners also enforce this before report publication. When a MatrixArk run declares a
baseline provider, baseline reader model, baseline embedding model, retrieved-event budget, or
reader context budget, `tools/run_locomo_ingest_once.py` automatically treats
`require_shared_oss_models=true`. The only escape hatch is
`--allow-shared-oss-model-drift`, which is diagnostic-only and must not be used for MatrixArk vs
the external OSS baseline quality or token-savings claims.

## Readiness Levels

Context benchmark readiness and the external baseline paper-comparable claims are separate gates:

- **Context benchmark readiness** requires Rust TemporalStore backend evidence: the report must show
  `all_pipelines_use_rust_temporalstore=true`, `python_only_diagnostic=false`,
  `rust_temporalstore_backend_ready=true`, and no direct-source scoring shortcut. Raw benchmark
  reports should also carry Context-event ingest evidence and positive ingested/retrieved source-set
  counts.
- **the external baseline paper-comparable evidence** additionally requires a real full dataset artifact, live
  open-source/GPT-4o-mini compatible reader calls, full Rust TemporalStore replay, passing full
  dataset thresholds, and archived report fields for dataset hash, model/provider, prompt,
  latencies, token reduction, and category breakdown.

Deterministic full-dataset reports can satisfy the first gate when Rust TemporalStore evidence is
present. They must keep `paper_comparable_claim_ready=false` until the live-reader/full-replay
archive gate passes.

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

Fair MatrixArk vs the external OSS baseline OSS comparison example:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile oss_reader_full \
  --input /tmp/longmemeval_s.json \
  --reader-mode open-source \
  --reader-provider-name matrixark-qwen-local \
  --reader-model qwen2.5:1.5b \
  --embedding-model matrixark-hash-embedding-32 \
  --max-events 64 \
  --reader-max-context-chars 4000 \
  --baseline-provider-name openexternal-baseline-qwen-local \
  --baseline-reader-model qwen2.5:1.5b \
  --baseline-embedding-model matrixark-hash-embedding-32 \
  --baseline-max-events 64 \
  --baseline-reader-max-context-chars 4000 \
  --reader-no-fallback

python3 tools/validate_oss_model_contract.py \
  --report /tmp/matrixark_longmemeval_report.json --label matrixark \
  --report /tmp/openexternal-baseline.json --label openexternal-baseline
```

LOCOMO and LongMemEval_s wrappers require the Rust TemporalStore backend by default. That path
converts benchmark cases to the Rust context JSONL contract and runs
`context_workflow_harness` through a real `TemporalEngine` before the deterministic Python
reader/scorer emits the final benchmark report. The runner also scores that exact converted subset
with the Python ranker and requires Rust case count, Hit@K, mean reciprocal rank, and zero-hit
query count to match the Python scorer within `--rust-temporalstore-score-tolerance` before marking
the backend ready. Reports also emit `all_pipelines_use_rust_temporalstore`; production benchmark
evidence requires that field to be `true`. A fixed `1e-6` numerical epsilon is always allowed so
Rust f32 report output does not fail exact source-rank conformance. Use `--skip-rust-temporalstore`
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

Rust-backed benchmark evidence must also prove the benchmark sources were ingested into Rust
TemporalStore context events before retrieval and scoring. Accepted reports therefore require
`rust_temporalstore_context_event_ingest_ready=true`,
`rust_temporalstore_direct_source_scoring=false`, positive ingested/retrieved source-set counts, and
positive retrieved block counts. Packed full replay is allowed only as a runtime optimization: it
must preserve source text and refs, run `context_workflow_harness`, and emit
`external_benchmark_rust_context_event_ingest=true` from the Rust harness output. Direct source
scoring remains diagnostic-only and cannot satisfy production or paper-comparable benchmark gates.

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
Use `tools/run_context_benchmarks_oss_reader_endpoint.sh` when validating the
the external baseline conformance reader profile against an existing OpenAI-compatible endpoint.
That path defaults to `external-baseline-gpt-4o-mini-reader` and `gpt-4o-mini`, and must
report `reader_open_source_calls > 0`.
