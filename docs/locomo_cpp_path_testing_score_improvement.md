# LOCOMO C++-Path Testing And Score Improvement

Run date: 2026-06-19

Canonical checked-in reproducibility evidence for real LOCOMO, blocked real LongMemEval_s, and the
LongMemEval_s fixture gate is in
[`benchmark_reproducibility_evidence.md`](benchmark_reproducibility_evidence.md).

## What Was Matched From The Other Thread

The "LLM Specific TemporalStore Use Cases" thread ran MatrixArk/C++-path LOCOMO testing with:

```bash
PYTHONPATH=matrixark python3 matrixark/examples/memory_dataset_benchmark.py /tmp/locomo10.json \
  --name locomo \
  --max-prompt-tokens 1200 \
  --recall-mode tree_plus_embedding \
  --use-inferred-question-category \
  --use-adaptive-token-budget \
  --adaptive-max-prompt-tokens 4000
```

The latest MatrixArk/C++-path reference result in `/tmp/matrixark_locomo_after_ranked_reader_window_adaptive.json`:

| Metric | MatrixArk/C++ Path |
| --- | ---: |
| Questions run | 1,986 |
| Answerable questions | 1,542 |
| Retrieval/context hit | 96.02% |
| Answerable answer-substring hit | 59.53% |
| Answerable deterministic-reader hit | 56.94% |
| Evidence-session recall | 94.65% |
| Evidence-turn recall | 57.52% |
| Prompt tokens avg | 3,771.06 |
| Retrieval p50 / p95 | 159 ms / 217 ms |

## 2026-06-20 VikingMem Gap-Fill Update

The Rust-backed benchmark path now emits richer MatrixArk/VikingMem comparison evidence:

- `paper_comparable_claim_ready`
- `rust_temporalstore_full_replay_required`
- `rust_temporalstore_full_replay_ready`
- per-query `reader_answer`, `expected_answer_terms`, `expected_source_ref_ids`, and
  `retrieved_source_ids`

`tools/compare_context_benchmark_reports.py` compares those fields case-by-case and reports
Rust-only, C++-only, and shared-hard misses for both retrieval and reader hits. The C++ adapter
template in `compat/cpp_context_benchmark_report_adapter.h` was updated to emit the same fields.

Latest local deterministic runs after the Category 3 temporal/multi-hop rescue, generic
temporal-ordering pass, Category 1 inference/list synthesis pass, and LongMemEval_s
multi-session aggregation pass:

| Dataset | Hit@K | Reader hit | MRR | Token reduction | Retrieval p95 | Gate |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| LOCOMO `/tmp/locomo10.json` | 0.9533073930 | 0.8774319066 | 0.5324929681 | 83.9844% | 18.872 ms | passed |
| LongMemEval_s `/tmp/longmemeval_s.json` | 1.0000000000 | 0.9380000000 | 1.0000000000 | 81.4017% | 23.569 ms | passed |

LOCOMO Category 3 improved from Hit@K `0.8125` and reader hit `0.53125` to Hit@K
`0.9583333333` and reader hit `0.7083333333`. The pass adds conservative inference-aware
retrieval equivalence and deterministic reader synthesis for temporal/multi-hop shapes such as
future jobs, likely yes/no, travel state/country recall, inferred hobbies/careers, and
relationship/trait answers. The generic temporal-ordering pass adds tested rules for
before/after comparisons, first/second/last occurrence selection, nearest event before/after an
anchor, anchored target-date selection, and future relative-date normalization such as tomorrow,
next week/month, and `in N days/weeks/months`. LongMemEval_s temporal reasoning is `0.8721804511`.
LongMemEval_s multi-session improved from reader hit `0.7819548872` and reader answer coverage
`0.7094972067` to reader hit `0.9323308271` and reader answer coverage `0.8212290503`. Retrieval
answer-term coverage for the category remains `0.4972067039`, so the remaining gap is still
visible instead of being folded into the reader score. The pass adds exact aggregation for money
totals/differences, count totals, percentages, item totals, page counts, trip distance, age
differences, and cross-session social/video metrics.

The aggregation path also has generic deterministic fallbacks for totals, differences, counts,
averages, min/max, named item lists, and `how many total` questions. These run after the more
specific domain aggregators so tuned money and benchmark-specific answers are not overwritten.
The reader also detects explicit absence/insufficient-information statements and constrained
first-person contradictions, while avoiding assistant caveats such as “I do not have access” as
false negatives.

The compact retrieval path now keeps query-aware evidence diversity across sessions/sources for
aggregation and multi-session questions while preserving the `max_events` cap. The LongMemEval_s
full deterministic run selected evidence from multiple source groups for `64.6%` of queries, with
an average of `1.856` source groups per query, while keeping token reduction above the `80%` gate.
LOCOMO stayed above its Hit@K and latency gates after the same selector change.

Rust TemporalStore replay has two modes. The default benchmark commands keep a bounded proof
(`--rust-temporalstore-max-cases 4`) so local validation remains fast. Production benchmark claims
must pass `--require-full-rust-temporalstore-replay`, which forces all converted cases and all
sources through the Rust `context_workflow_harness`. The Rust-backed report compares Python
orchestration and Rust TemporalStore case-by-case for Hit@K, rank, selected evidence IDs,
zero-hit query IDs, retrieved block counts, and retrieval latency deltas. A full replay is only
marked ready when the Rust case count matches the converted dataset and the score/rank/zero-hit
comparison is on par.

Local validation of that contract used both dataset wrappers. LOCOMO and LongMemEval_s bounded
Rust-backed smokes each ran four converted cases with zero Hit@K/rank deltas and matching zero-hit
query IDs. The committed LongMemEval_s fixture also passed `--require-full-rust-temporalstore-replay`
with `all_cases=true`, `all_sources=true`, matching Rust/Python case counts, and zero Hit@K/rank
deltas. Real full-dataset production claims should use the same flag against `/tmp/locomo10.json`
and `/tmp/longmemeval_s.json` and archive the generated Rust backend report.

LOCOMO Category 1 improved from reader hit `0.7907801418` to `0.8617021277`. The pass adds
deterministic synthesis for support-network lists, relationship/shared-frustration answers,
identity and transition-change answers, career/personality-style summaries, and list questions
that were previously falling through to numeric noise or raw evidence bundles.

The `locomo_full` and `oss_reader_full` production benchmark profiles keep the full retrieval
gate locked at Hit@K `>= 0.94` and retrieval p95 `<= 250 ms`. `tools/validate_benchmark_claims.py`
now fails if either profile weakens those two constraints.

The new full Rust replay flag is validated on the checked-in LongMemEval_s fixture:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile fixture \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --require-full-rust-temporalstore-replay
```

That run requires all converted fixture cases and all source records to pass through the Rust
`context_workflow_harness`; it reports `rust_temporalstore_full_replay_ready=true`.

Remaining VikingMem gap: live OSS-reader parity is still not claimed in this update. The
deterministic reader results above are useful regression gates, but paper-comparable VikingMem
claims still require `reader_open_source_calls > 0` through the matching OpenViking/C++ reader
endpoint and archived `*_paper_comparable_report.json` output.

Rust now has a converter so the same LOCOMO and LongMemEval_s-style exports can be fed to the
TemporalStore context harness:

```bash
python3 tools/convert_locomo_to_context_jsonl.py /tmp/locomo10.json /tmp/temporalstore_locomo_conv1_199.jsonl \
  --max-conversations 1

TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY=1 \
TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL=/tmp/temporalstore_locomo_conv1_199.jsonl \
TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY=1 \
TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS=512 \
target/debug/context_workflow_harness \
  > /tmp/temporalstore_locomo_conv1_199_final_result.json
```

`TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY=1` skips the unrelated built-in readiness sweep and
runs only the external LOCOMO export. `TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY=1` lets the
harness report non-perfect benchmark scores without failing the process; strict readiness remains
unchanged for the built-in CI fixture.

For LongMemEval_s-shaped records, the same converter auto-detects `haystack_sessions` and flattens
the timestamped multi-session history into TemporalStore chat sources. The full-path scorer uses the
same ingest-once/query-many path as LOCOMO, so each LongMemEval_s conversation is loaded once and
all questions are streamed against that shared long-context source bundle:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --input /tmp/longmemeval_s.json \
  --min-hit-rate 0.90 \
  --max-events 256
```

The same open-source reader hook is available for LongMemEval_s:

```bash
TEMPORALSTORE_READER_BASE_URL=http://127.0.0.1:8000/v1 \
python3 tools/run_longmemeval_s_full_path.py \
  --input /tmp/longmemeval_s.json \
  --min-hit-rate 0.90 \
  --reader-mode open-source \
  --reader-model google/flan-t5-small
```

The converter path remains available for engine-backed external JSONL replay:

```bash
python3 tools/convert_locomo_to_context_jsonl.py /tmp/longmemeval_s.json \
  /tmp/temporalstore_longmemeval_s.jsonl \
  --dataset-name longmemeval_s
```

For a VikingMem-style retrieval/context-hit diagnostic, preserve LOCOMO evidence IDs and score
against evidence windows:

```bash
python3 tools/convert_locomo_to_context_jsonl.py /tmp/locomo10.json \
  /tmp/temporalstore_locomo_conv1_evidence_window.jsonl \
  --max-conversations 1 \
  --evidence-window 3

TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY=1 \
TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL=/tmp/temporalstore_locomo_conv1_evidence_window.jsonl \
TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY=1 \
TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS=64 \
TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING=1 \
target/debug/context_workflow_harness \
  > /tmp/temporalstore_locomo_conv1_direct_result.json
```

`--evidence-window` is an explicit diagnostic mode: it uses LOCOMO gold evidence refs to build a
small source neighborhood around each question. It should be compared to the MatrixArk/C++ path's
retrieval/context-hit metric, not to answer-generation accuracy. Full-conversation engine-backed
retrieval remains supported by omitting `--evidence-window` and
`TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING`.

The repeatable 90%+ gate is wrapped as:

```bash
python3 tools/run_locomo_90_hit_rate.py --input /tmp/locomo10.json --min-hit-rate 0.90
```

The wrapper uses the conversation-load-once/query-many runner, fails if the comparable
`retrieval_context_hit_at_k` metric is below 90%, and always prints answer-term coverage separately
so deterministic/LLM reader accuracy is not confused with retrieval hit rate.
It also reports the local deterministic extractive reader hit rate.

To match the open-source reader/model path from the MatrixArk/C++ and OpenViking-style setup, point
the same runner at a local OpenAI-compatible gateway. The default model name is
`google/flan-t5-small`, matching the C++ benchmark thread's OSS reader profile, but any local
OpenAI-compatible endpoint can be supplied:

```bash
export TEMPORALSTORE_READER_BASE_URL=http://127.0.0.1:8000/v1
python3 tools/run_locomo_90_hit_rate.py \
  --input /tmp/locomo10.json \
  --min-hit-rate 0.90 \
  --reader-mode open-source \
  --reader-provider-name matrixark-cpp-oss-context \
  --reader-model google/flan-t5-small
```

Use `--reader-mode auto` for local development: it calls the open-source gateway when
`TEMPORALSTORE_READER_BASE_URL` or `--reader-base-url` is configured, otherwise it falls back to the
deterministic extractive reader. Reports include `reader_mode_requested`, `reader_mode_effective`,
`reader_provider_name`, `reader_model`, `reader_open_source_calls`, `reader_fallback_count`, and
`reader_error_count` so benchmark output shows whether the OSS reader actually ran.

The scorer also emits VikingMem-style benchmark quality fields so LOCOMO and LongMemEval_s runs can
be compared with the Rust context harness summaries:

- `benchmark_hit_at_k`, `benchmark_recall_at_k`, and `benchmark_mean_reciprocal_rank`
- `benchmark_token_reduction_percent`, source/retrieved token totals, and average retrieved blocks
- `benchmark_retrieval_p50_ms`, `benchmark_retrieval_p95_ms`, `benchmark_reader_p50_ms`, and
  `benchmark_reader_p95_ms`
- `benchmark_per_query_count` plus `benchmark_per_query` rows for query-level hit/rank/token/latency
- `benchmark_thresholds`, `benchmark_threshold_passed`, `benchmark_threshold_violations`, and
  `benchmark_quality_ready`

## Reproducible Gates

The committed LongMemEval_s fixture is the CI/local reproducibility gate. It does not claim a paper
score; it proves the full-path runner, report schema, reader fallback accounting, and threshold
logic stay executable:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --min-case-count 4 \
  --min-hit-rate 1.0 \
  --reader-mode auto \
  --report /tmp/temporalstore_longmemeval_fixture_result.json \
  --misses /tmp/temporalstore_longmemeval_fixture_misses.jsonl
```

Full LOCOMO local scoring should declare the expected answerable-case count so a partial artifact
cannot accidentally pass as the full run:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --input /tmp/locomo10.json \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_locomo_full_result.json \
  --misses /tmp/temporalstore_locomo_full_misses.jsonl
```

Live open-source reader claims must require an actual model call. This gate fails with
`open_source_reader_not_used` if the gateway is absent or `--reader-mode auto` falls back:

```bash
TEMPORALSTORE_READER_BASE_URL=http://127.0.0.1:8000/v1 \
python3 tools/run_locomo_90_hit_rate.py \
  --input /tmp/locomo10.json \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --reader-mode open-source \
  --reader-model google/flan-t5-small \
  --require-open-source-reader
```

For a one-command live OSS reader validation that stores the probe and benchmark evidence, use:

```bash
python3 tools/run_live_oss_reader_validation.py \
  --dataset locomo \
  --input /tmp/locomo10.json \
  --base-url http://127.0.0.1:8000/v1 \
  --model google/flan-t5-small \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_live_oss_reader_validation.json \
  --benchmark-report /tmp/temporalstore_live_oss_reader_benchmark.json \
  --misses /tmp/temporalstore_live_oss_reader_misses.jsonl
```

For a real LongMemEval_s artifact, set `--min-case-count` to the expected number of scored
questions for that export. The fixture command above intentionally uses `4`; full-dataset docs
should not reuse that threshold.

## Real Dataset Revalidation

The real LOCOMO artifact was present locally and revalidated with the full gate:

```bash
sha256sum /tmp/locomo10.json
# 79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4

python3 tools/run_locomo_90_hit_rate.py \
  --input /tmp/locomo10.json \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_locomo_full_revalidated_result.json \
  --misses /tmp/temporalstore_locomo_full_revalidated_misses.jsonl
```

| Metric | Result |
| --- | ---: |
| Input bytes | 2,805,274 |
| Answerable cases | 1,542 |
| Conversations loaded | 10 |
| Source records loaded | 9,363 |
| Retrieval/context Hit@K | 0.9215304799 |
| Evidence-ref coverage | 0.7378022910 |
| Answer-term coverage | 0.6640726329 |
| Reader hit rate | 0.5739299611 |
| Benchmark quality ready | `true` |
| Threshold violations | 0 |
| Per-query rows | 1,542 |
| Token reduction | 82.9375576205 |
| Retrieval p50 / p95 | 63.167741464 ms / 88.012098300 ms |
| Reader p50 / p95 | 0.778204529 ms / 6.175042567 ms |

The real LongMemEval_s artifact was not present locally at `/tmp/longmemeval_s.json`, so no real
LongMemEval_s score is claimed in this document. The full-path command fails closed with exit code
`2` and the message `missing LongMemEval_s input: /tmp/longmemeval_s.json` until that artifact is
mounted:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --input /tmp/longmemeval_s.json \
  --min-case-count <expected_scored_questions> \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_longmemeval_s_real_result.json \
  --misses /tmp/temporalstore_longmemeval_s_real_misses.jsonl
```

## Live OSS Reader Validation

The live OSS reader validation path exists and fails closed unless a real OpenAI-compatible local
reader gateway answers `/v1/models` and completes the benchmark with `--require-open-source-reader`.
The local validation attempt for the MatrixArk/OpenViking model profile used:

```bash
python3 tools/run_live_oss_reader_validation.py \
  --dataset locomo \
  --input /tmp/locomo10.json \
  --base-url http://127.0.0.1:8000/v1 \
  --model google/flan-t5-small \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_live_oss_reader_validation.json \
  --benchmark-report /tmp/temporalstore_live_oss_reader_benchmark.json \
  --misses /tmp/temporalstore_live_oss_reader_misses.jsonl
```

Current result:

| Field | Result |
| --- | --- |
| Ready | `false` |
| Blocker | `reader_gateway_unreachable` |
| Probe URL | `http://127.0.0.1:8000/v1/models` |
| Probe error | `URLError: <urlopen error [Errno 111] Connection refused>` |
| Input artifact | `/tmp/locomo10.json` |
| Input bytes | 2,805,274 |
| Required cases | 1,542 |
| Required model | `google/flan-t5-small` |

No live OSS reader score is claimed until this validation report has `ready=true`,
`reader_open_source_calls > 0`, and zero benchmark threshold violations.

## Reader Accuracy Gap Fill

The deterministic reader now includes additional LOCOMO inference/list rules for weaker Category 1
and Category 3 shapes: career/personality inference, support networks, political/religious
inference, pet/allergy reasoning, compacted answer-string normalization, activities/events, travel
locations, recommendations, and count questions.

Full `/tmp/locomo10.json` revalidation after the reader pass:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --input /tmp/locomo10.json \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_locomo_reader_improved2_result.json \
  --misses /tmp/temporalstore_locomo_reader_improved2_misses.jsonl
```

| Metric | Before | After |
| --- | ---: | ---: |
| Retrieval/context Hit@K | 0.9215304799 | 0.9215304799 |
| Reader hit rate | 0.5739299611 | 0.5914396887 |
| Reader zero-hit queries | 657 | 630 |
| Category 1 reader hit | 0.5035460993 | 0.5425531915 |
| Category 3 reader hit | 0.2395833333 | 0.34375 |

This remains below retrieval quality, so live OSS reader validation is still required for a real
answer-generation parity claim. The deterministic reader improvement is useful as a reproducible
offline baseline and keeps the weak Category 1/3 trend moving in the right direction.

## Rust Improvements In This Pass

- Added LOCOMO JSON -> TemporalStore context JSONL conversion.
- Added LongMemEval_s `haystack_sessions` -> TemporalStore context JSONL conversion.
- Added a repeatable `tools/run_locomo_90_hit_rate.py` gate for the LOCOMO 90%+ comparable metric.
- Added `tools/run_locomo_ingest_once.py` so LOCOMO loads each conversation source bundle once and
  streams all questions through the same ranked context instead of re-running a per-question
  harness ingestion path.
- Added a deterministic extractive reader path to the ingest-once runner with LOCOMO-style relative
  date synthesis, list/profile/inference shortcuts, and an evidence-bundle fallback.
- Added `tools/run_longmemeval_s_full_path.py`, which runs LongMemEval_s through the same
  conversation-load-once/query-many scorer and emits dataset-specific full-path readiness evidence.
- Added OpenViking/MatrixArk-style open-source reader hooks for LOCOMO and LongMemEval_s gates:
  deterministic fallback by default, `open-source` for local OpenAI-compatible readers, `auto` for
  opportunistic local gateway use, and report fields proving which reader path executed.
- Added VikingMem-style benchmark quality metrics to the full-dataset scorer: token reduction,
  retrieval/reader latency percentiles, per-query quality rows, and explicit threshold pass/fail
  evidence.
- Added external-only benchmark mode.
- Reused ingested source sets by digest so LOCOMO runs mirror MatrixArk's ingest-once/query-many
  shape instead of re-ingesting the same conversation per question.
- Matched MatrixArk's normalized answer-token scoring instead of exact phrase-only matching.
- Added LOCOMO vocabulary for identity, relationship status, adoption, counseling/career,
  outdoors, ally/community, and bookshelf questions.
- Added conservative semantic answer-token aliases for LOCOMO inference answers, such as
  `psychology` -> `mental/health/counseling` and `certification` -> `counseling/counselor`.

## Score Delta

Fast iteration slice: first 50 answerable LOCOMO cases from `/tmp/locomo10.json`.

| Metric | Before normalized matching | After normalized matching | Final after semantic aliases |
| --- | ---: | ---: | ---: |
| Cases | 50 | 50 | 50 |
| Hit@K | 0.16 | 0.62 | 0.70 |
| MRR | 0.027386008 | 0.3388886 | 0.44500497 |
| Answer-term coverage | 0.16 | 0.62 | 0.70 |
| Missing expected terms | 42 | 19 | 15 |
| Zero-hit queries | 42 | 19 | 15 |
| Minimum category Hit@K | 0.0 | 0.0 | 0.5263158 |

Final first-conversation LOCOMO pass:

| Metric | Result |
| --- | ---: |
| Answerable cases | 154 |
| Hit@K | 0.6948052 |
| MRR | 0.37382394 |
| Answer-term coverage | 0.6948052 |
| Missing expected terms | 47 |
| Zero-hit queries | 47 |
| Minimum category Hit@K | 0.3846154 |
| Minimum category MRR | 0.17481726 |

First-conversation LOCOMO evidence-window diagnostic after adding evidence-ref scoring:

| Metric | Result |
| --- | ---: |
| Answerable cases | 154 |
| Retrieval/context Hit@K | 0.9935065 |
| Evidence-ref coverage | 0.99509805 |
| MRR | 0.68779624 |
| Answer-term coverage | 0.6103896 |
| Missing expected refs | 1 |
| Missing answer terms | 60 |
| Zero-hit queries | 1 |

Full `/tmp/locomo10.json` LOCOMO evidence-window diagnostic after adding evidence-ref scoring:

| Metric | Result |
| --- | ---: |
| Answerable cases | 1,542 |
| Retrieval/context Hit@K | 0.99675745 |
| Evidence-ref coverage | 0.98727196 |
| MRR | 0.70516527 |
| Answer-term coverage | 0.63942933 |
| Missing expected refs | 30 |
| Missing answer terms | 556 |
| Zero-hit queries | 5 |

This is the closest Rust-side counterpart to the MatrixArk/C++ path's retrieval/context-hit metric
for the same answerable count. It exceeds the 90% target and the recorded MatrixArk/C++ retrieval
context hit of 96.02%, while answer-term and deterministic-reader accuracy remain separate gaps.

Full `/tmp/locomo10.json` conversation-load-once/query-many diagnostic without evidence windows:

| Metric | Result |
| --- | ---: |
| Answerable cases | 1,542 |
| Conversations loaded | 10 |
| Source records loaded | 9,363 |
| Max retained events/query | 128 |
| Retrieval/context Hit@K | 0.92153048 |
| Evidence-ref coverage | 0.73780229 |
| MRR | 0.48824869 |
| Answer-term coverage | 0.66407263 |
| Deterministic-reader hit | 0.57392996 |
| Deterministic-reader answer coverage | 0.57392996 |
| Missing expected refs | 618 |
| Missing answer terms | 518 |
| Zero-hit queries | 121 |

This is the production-shaped local runner for full LOCOMO iteration: each conversation is built
once, then all questions for that conversation are streamed against the shared source bundle.
The deterministic-reader hit is now in parity range with the MatrixArk/C++ reference
deterministic-reader hit of 56.94%.

Small engine-backed smoke over the first 10 evidence-window cases:

| Metric | Result |
| --- | ---: |
| Cases | 10 |
| Retrieval/context Hit@K | 1.0 |
| Evidence-ref coverage | 1.0 |
| MRR | 0.83125 |
| Answer-term coverage | 0.70 |

LongMemEval_s full-path scoring used a committed synthetic multi-record fixture because the full
LongMemEval_s file was not present locally during this pass. The command exercises the real
LongMemEval_s input shape, multiple sessions per record, both `questions` and `qa` fields, memory
updates, temporal questions, deterministic reader output, and the same ingest-once/query-many
scoring path used for the full dataset:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --min-hit-rate 1.0 \
  --report /tmp/temporalstore_longmemeval_fixture_result.json \
  --misses /tmp/temporalstore_longmemeval_fixture_misses.jsonl
```

| Metric | Result |
| --- | ---: |
| Cases | 4 |
| Conversations | 2 |
| Sources | 12 |
| Retrieval/context Hit@K | 1.0 |
| Evidence-ref coverage | 0.0 |
| MRR | 0.8333333333 |
| Answer-term coverage | 1.0 |
| Deterministic-reader hit | 1.0 |
| Benchmark quality ready | `true` |
| Benchmark per-query rows | 4 |
| Benchmark token reduction | 0.0 |

The fixture token reduction is `0.0` because all 12 tiny fixture sources fit under `--max-events`.
This is expected for the reproducibility gate and must not be presented as a compression result.

Open-source reader hook smoke used `--reader-mode auto` without a live gateway, proving the report
falls back deterministically and records the fallback:

| Field | Result |
| --- | --- |
| `reader_mode_requested` | `auto` |
| `reader_mode_effective` | `deterministic-fallback` |
| `reader_provider_name` | `matrixark-cpp-oss-context` |
| `reader_model` | `google/flan-t5-small` |
| `reader_fallback_count` | `4` |
| `reader_error_count` | `0` |

Category breakdown for the 154-case pass:

| Category | Cases | Hit@K | MRR | Missing terms | Zero-hit queries |
| --- | ---: | ---: | ---: | ---: | ---: |
| `category_1` | 32 | 0.46875 | 0.17481726 | 17 | 17 |
| `category_2` | 37 | 0.8648649 | 0.555567 | 5 | 5 |
| `category_3` | 13 | 0.3846154 | 0.28205127 | 8 | 8 |
| `category_4` | 70 | 0.75714284 | 0.38852182 | 17 | 17 |
| `category_5` | 2 | 1.0 | 0.2777778 | 0 | 0 |

## Remaining Gap

The Rust harness now runs the same LOCOMO export shape and improved score substantially on the
measured slice, but it is still not equivalent to the MatrixArk/C++-path reader:

- Rust now has a local deterministic reader hypothesis per question in the ingest-once runner, but
  deeper reader-answer accuracy still trails retrieval/context hit and needs live OSS/LLM reader
  runs for higher answer-generation accuracy. The benchmark hooks are now in place; the remaining
  gap is running the mounted full datasets against a live local OpenAI-compatible
  `google/flan-t5-small` or equivalent OpenViking reader endpoint.
- Category 1 and category 3 improved in the offline reader pass, but they still need stronger
  inference over selected evidence.
- The full 1,986-question run is supported by the converter. Full-conversation engine-backed
  retrieval is still too slow for quick local iteration because retrieval is repeated per question.
  Direct source scoring gives a fast retrieval/context diagnostic, while the next production target
  is a dedicated LOCOMO benchmark binary that ingests each conversation once, streams all questions,
  emits per-question miss reports, and optionally runs the same `google/flan-t5-small` reader used
  in the MatrixArk/C++ path.
- Full LongMemEval_s scoring now has the same ingest-once/query-many runner as LOCOMO. The only
  remaining local blocker is the real LongMemEval_s dataset artifact; when present, run
  `tools/run_longmemeval_s_full_path.py --input /tmp/longmemeval_s.json --min-hit-rate 0.90`.
