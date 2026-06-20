# Benchmark Reproducibility Evidence

Validation time: 2026-06-20T05:53:06Z

Runner revision: `84f77ef`

This page records benchmark evidence from the checked-in TemporalStore runners. It separates real
dataset scores from fixture gates so fixture results are not presented as paper-equivalent LOCOMO or
LongMemEval_s scores.

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
  --input /tmp/locomo10.json \
  --min-case-count 1542 \
  --min-hit-rate 0.90 \
  --report /tmp/temporalstore_locomo_reader_improved2_result.json \
  --misses /tmp/temporalstore_locomo_reader_improved2_misses.jsonl
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
| Report JSON | `/tmp/temporalstore_locomo_reader_improved2_result.json` |
| Misses JSONL | `/tmp/temporalstore_locomo_reader_improved2_misses.jsonl` |

Thresholds:

| Threshold | Value |
| --- | ---: |
| `min_case_count` | 1542 |
| `min_hit_at_k` | 0.90 |
| `min_reader_hit_rate` | 0.0 |
| `min_token_reduction_percent` | 0.0 |
| `max_retrieval_p95_ms` | 1000.0 |
| `max_reader_p95_ms` | 30000.0 |
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
| Retrieval p50 / p95 | 72.555758001 ms / 93.420915731 ms |
| Reader p50 / p95 | 0.680133002 ms / 5.866982782 ms |

## LongMemEval_s Full Dataset

Status: `blocked`

The real LongMemEval_s artifact is not present at `/tmp/longmemeval_s.json`, so no real
LongMemEval_s score is claimed.

Attempted command:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --input /tmp/longmemeval_s.json \
  --min-case-count 1 \
  --min-hit-rate 0.90 \
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
`--min-case-count` set to the expected scored-question count for that export.

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
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --min-case-count 4 \
  --min-hit-rate 1.0 \
  --reader-mode auto \
  --report /tmp/temporalstore_longmemeval_fixture_current_result.json \
  --misses /tmp/temporalstore_longmemeval_fixture_current_misses.jsonl
```

Run configuration:

| Field | Value |
| --- | --- |
| Mode | `conversation_load_once_query_many` |
| Model profile | `matrixark-cpp-oss-context` |
| Reader mode requested | `auto` |
| Reader mode effective | `deterministic-fallback` |
| Reader model | `google/flan-t5-small` |
| Report JSON | `/tmp/temporalstore_longmemeval_fixture_current_result.json` |
| Misses JSONL | `/tmp/temporalstore_longmemeval_fixture_current_misses.jsonl` |

Thresholds:

| Threshold | Value |
| --- | ---: |
| `min_case_count` | 4 |
| `min_hit_at_k` | 1.0 |
| `min_reader_hit_rate` | 0.0 |
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
| Retrieval p50 / p95 | 0.306498492 ms / 0.332475768 ms |
| Reader p50 / p95 | 0.158256502 ms / 0.629811781 ms |

Fixture token reduction is `0.0` because the tiny fixture fits under `--max-events`; it is not a
compression or prompt-budget result.
