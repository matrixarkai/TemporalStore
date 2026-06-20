# Benchmark Reproducibility Evidence

Validation time: 2026-06-20T06:36:00Z

Runner revision: threshold-policy update in this commit

This page records benchmark evidence from the checked-in TemporalStore runners. It separates real
dataset scores from fixture gates so fixture results are not presented as paper-equivalent LOCOMO or
LongMemEval_s scores.

Threshold policy is defined in [benchmark_threshold_policy.md](benchmark_threshold_policy.md).
Fixture gates and full-dataset production gates intentionally use different profiles.
The packaged Docker/open-model path is documented in
[context_benchmarks_docker_open_model.md](context_benchmarks_docker_open_model.md).

Claim level: LOCOMO deterministic full-dataset gate evidence plus LongMemEval_s fixture evidence.
This page does not claim production parity because the live OSS reader path and the real
LongMemEval_s artifact have not both passed their full-dataset thresholds in this evidence set.

## LOCOMO Full Dataset

Status: `ready`

Input artifact:

| Field | Value |
| --- | --- |
| Path | `/tmp/locomo10.json` |
| SHA-256 | `79fa87e90f04081343b8c8debecb80a9a6842b76a7aa537dc9fdf651ea698ff4` |
| Bytes | `2,805,274` |

Command:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --threshold-profile locomo_full \
  --input /tmp/locomo10.json \
  --report /tmp/temporalstore_locomo_threshold_policy_result.json \
  --misses /tmp/temporalstore_locomo_threshold_policy_misses.jsonl
```

Run configuration:

| Field | Value |
| --- | --- |
| Mode | `conversation_load_once_query_many` |
| Model profile | `matrixark-cpp-oss-context` |
| Reader mode requested | `deterministic` |
| Reader mode effective | `deterministic` |
| Reader model | `google/flan-t5-small` |
| Max events | `128` |
| Report JSON | `/tmp/temporalstore_locomo_threshold_policy_result.json` |
| Misses JSONL | `/tmp/temporalstore_locomo_threshold_policy_misses.jsonl` |

Thresholds:

| Threshold | Value |
| --- | ---: |
| `threshold_profile` | `locomo_full` |
| `min_case_count` | 1542 |
| `min_hit_at_k` | 0.90 |
| `min_reader_hit_rate` | 0.58 |
| `min_token_reduction_percent` | 80.0 |
| `max_retrieval_p95_ms` | 250.0 |
| `max_reader_p95_ms` | 50.0 |
| `require_open_source_reader` | `false` |

Output:

| Metric | Value |
| --- | ---: |
| Case count | 1,542 |
| Conversation count | 10 |
| Source record count | 9,363 |
| Retrieval/context Hit@K | 0.9215304799 |
| Mean reciprocal rank | 0.4883179758 |
| Evidence-ref coverage | 0.7382265592 |
| Answer-term coverage | 0.6647211414 |
| Reader hit rate | 0.5914396887 |
| Reader answer coverage | 0.5914396887 |
| Benchmark quality ready | `true` |
| Threshold violations | `[]` |
| Per-query rows | 1,542 |
| Token reduction | 82.9375085567 |
| Retrieval p50 / p95 | 71.491033479 ms / 89.486754028 ms |
| Reader p50 / p95 | 0.654528034 ms / 5.510890001 ms |

Follow-up context gap-fill validation regenerated the report with per-category weak-slice fields:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --threshold-profile locomo_full \
  --input /tmp/locomo10.json \
  --report /tmp/temporalstore_locomo_context_gapfix_result.json \
  --misses /tmp/temporalstore_locomo_context_gapfix_misses.jsonl
```

The regenerated report adds `category_breakdown`, `weak_category_count`, `weak_categories`, and
`weak_category_policy`. In that run, the overall LOCOMO gate still passed at
`benchmark_hit_at_k = 0.9215304798962386`, with `weak_category_count = 5`. The weakest retrieval
slice was `category_3` at `hit_rate = 0.78125`; reader hit-rate was also below the policy threshold
for categories 1, 2, 3, and 5. This is diagnostic evidence for the next reader/retrieval quality
pass, not a new paper-score claim.

## LongMemEval_s Full Dataset

Status: `blocked`

The real LongMemEval_s artifact is not present at `/tmp/longmemeval_s.json`, so no real
LongMemEval_s score is claimed.

Attempted command:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile longmemeval_full \
  --input /tmp/longmemeval_s.json \
  --report /tmp/temporalstore_longmemeval_real_repro_result.json \
  --misses /tmp/temporalstore_longmemeval_real_repro_misses.jsonl
```

Blocked output:

| Field | Value |
| --- | --- |
| Exit code | `2` |
| Error | `missing LongMemEval_s input: /tmp/longmemeval_s.json` |
| Input hash | unavailable because the artifact is absent |
| Case count | unavailable because the artifact is absent |
| Report JSON | unavailable because the runner exits before scoring |

To record the real LongMemEval_s evidence, mount the artifact and rerun the command with
`--threshold-profile longmemeval_full`. Override `--min-case-count` only if the mounted export has a
documented scored-question count higher than the profile floor.

## LOCOMO Reader Gap-Fill Validation

Status: `accuracy-improved-threshold-blocked`

This run validates deterministic reader changes for the known weak LOCOMO categories. It is not a
replacement production gate result because local retrieval latency exceeded the strict p95 threshold
on this machine during the run.

Command:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --threshold-profile locomo_full \
  --input /tmp/locomo10.json \
  --report /tmp/temporalstore_locomo_reader_gapfill_full2_result.json \
  --misses /tmp/temporalstore_locomo_reader_gapfill_full2_misses.jsonl
```

Output:

| Metric | Previous LOCOMO full | Reader gap-fill run |
| --- | ---: | ---: |
| Case count | 1,542 | 1,542 |
| Retrieval/context Hit@K | 0.9215304799 | 0.9215304799 |
| Reader hit rate | 0.5914396887 | 0.6147859922 |
| Token reduction | 82.9375085567 | 82.9342781992 |
| Retrieval p95 | 89.486754028 ms | 269.787499326 ms |
| Reader p95 | 5.510890001 ms | 15.875367762 ms |
| Threshold violations | `[]` | `["retrieval_p95_above_max"]` |

Category reader hit-rate movement:

| Category | Previous | Reader gap-fill |
| --- | ---: | ---: |
| `category_1` | 0.5425531915 | 0.5567375887 |
| `category_2` | 0.2024922118 | 0.2803738318 |
| `category_3` | 0.3437500000 | 0.3645833333 |
| `category_4` | 0.7847800238 | 0.7907253270 |

Claim level: deterministic reader accuracy improvement only. This does not claim production parity,
live OSS-reader parity, or a new production LOCOMO gate pass because retrieval p95 did not satisfy
`locomo_full` in this run.

## LongMemEval_s Fixture Gate

Status: `ready`

This fixture is a reproducibility gate for the LongMemEval_s runner shape. It is not a real
LongMemEval_s score.

Input artifact:

| Field | Value |
| --- | --- |
| Path | `tools/fixtures/longmemeval_s_full_path_fixture.json` |

Command:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile fixture \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --reader-mode auto \
  --report /tmp/temporalstore_longmemeval_fixture_threshold_policy_result.json \
  --misses /tmp/temporalstore_longmemeval_fixture_threshold_policy_misses.jsonl
```

Run configuration:

| Field | Value |
| --- | --- |
| Mode | `conversation_load_once_query_many` |
| Model profile | `matrixark-cpp-oss-context` |
| Reader mode requested | `auto` |
| Reader mode effective | `deterministic-fallback` |
| Reader model | `google/flan-t5-small` |
| Report JSON | `/tmp/temporalstore_longmemeval_fixture_threshold_policy_result.json` |
| Misses JSONL | `/tmp/temporalstore_longmemeval_fixture_threshold_policy_misses.jsonl` |

Thresholds:

| Threshold | Value |
| --- | ---: |
| `threshold_profile` | `fixture` |
| `min_case_count` | 4 |
| `min_hit_at_k` | 1.0 |
| `min_reader_hit_rate` | 1.0 |
| `min_token_reduction_percent` | 0.0 |
| `max_retrieval_p95_ms` | 1000.0 |
| `max_reader_p95_ms` | 30000.0 |
| `require_open_source_reader` | `false` |

Output:

| Metric | Value |
| --- | ---: |
| Case count | 4 |
| Conversation count | 2 |
| Source record count | 12 |
| Retrieval/context Hit@K | 1.0 |
| Reader hit rate | 1.0 |
| Benchmark quality ready | `true` |
| Threshold violations | `[]` |
| Per-query rows | 4 |
| Token reduction | 0.0 |
| Retrieval p50 / p95 | 0.322518987 ms / 0.365485722 ms |
| Reader p50 / p95 | 0.185855024 ms / 0.714177469 ms |

Fixture token reduction is `0.0` because the tiny fixture fits under `--max-events`; it is not a
compression or prompt-budget result.
