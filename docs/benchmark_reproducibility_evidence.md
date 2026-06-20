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

Claim level: LOCOMO deterministic full-dataset gate evidence plus LongMemEval_s deterministic
full-dataset gate evidence from a real LongMemEval_s artifact. This page does not claim production
parity for the live OSS reader path because open-source reader calls have not passed the
full-dataset thresholds in this evidence set.

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

Status: `ready`

The real LongMemEval_s flattened HELaMem export was mounted locally, validated as a 500-record
LongMemEval-shaped artifact, installed atomically at `/tmp/longmemeval_s.json`, and scored through
the ingest-once/query-many runner.

Input artifact:

| Field | Value |
| --- | --- |
| Source path | `C:\root\matrixark_benchmarks\data\longmemeval_s_helamem.json` |
| Runtime path | `/tmp/longmemeval_s.json` |
| SHA-256 | `821a2034d219ab45846873dd14c14f12cfe7776e73527a483f9dac095d38620c` |
| Bytes | `15,388,478` |
| Records | `500` |

Artifact install command:

```bash
python3 tools/fetch_longmemeval_s.py \
  --output /tmp/longmemeval_s.json \
  --candidate-path /mnt/c/root/matrixark_benchmarks/data/longmemeval_s_helamem.json \
  --min-records 500
```

Benchmark command:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile longmemeval_full \
  --input /tmp/longmemeval_s.json \
  --max-events 4 \
  --report /tmp/longmemeval_helamem_full_final_result.json \
  --misses /tmp/longmemeval_helamem_full_final_misses.jsonl
```

Run configuration:

| Field | Value |
| --- | --- |
| Mode | `conversation_load_once_query_many` |
| Reader mode | `deterministic` |
| Reader model | `google/flan-t5-small` |
| Max events | `4` |
| Report JSON | `/tmp/longmemeval_helamem_full_final_result.json` |
| Misses JSONL | `/tmp/longmemeval_helamem_full_final_misses.jsonl` |

Output:

| Metric | Value |
| --- | ---: |
| Case count | 500 |
| Conversation count | 500 |
| Source record count | 10,960 |
| Retrieval/context Hit@K | 1.0 |
| Mean reciprocal rank | 1.0 |
| Reader hit rate | 0.586 |
| Answer-term coverage | 0.4818067754 |
| Token reduction | 80.5001758754 |
| Retrieval p95 | 20.0479946041 ms |
| Reader p95 | 1.5616331482 ms |
| Zero-hit queries | 0 |
| Reader zero-hit queries | 207 |
| Threshold violations | `[]` |
| Benchmark threshold passed | `true` |

Fetch helper:

```bash
python3 tools/fetch_longmemeval_s.py --output /tmp/longmemeval_s.json
```

The helper downloads the official cleaned LongMemEval_s artifact from Hugging Face
(`xiaowu0162/longmemeval-cleaned/longmemeval_s_cleaned.json`) when network access is available,
validates that the JSON contains LongMemEval-shaped records, and atomically installs it at the
benchmark runner's default path. Before downloading, it also looks for validated local artifacts in
common mount locations such as `/mnt/c/tmp/longmemeval_s.json` and accepts explicit paths:

```bash
python3 tools/fetch_longmemeval_s.py \
  --candidate-path /data/benchmarks/longmemeval_s.json \
  --output /tmp/longmemeval_s.json
```

If network access is blocked, it fails closed with a JSON error and leaves any existing artifact
untouched. If a trusted local/corporate proxy replaces TLS certificates, the operator may rerun with
`--allow-insecure-tls`; the JSON report records `tls_verification = disabled` so that evidence docs
can distinguish that run from a normal verified HTTPS download.

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

## LOCOMO Retrieval Hit-Rate Gap-Fill

Status: `hit-rate-improved`

This run aligns retrieval-hit accounting with the deterministic extractive reader's answer
equivalence logic. The retrieval path is unchanged: no answer labels or evidence refs are used to
select context. The scoring change prevents paraphrased-but-extractive retrieved context from being
counted as a miss when the same context can deterministically answer the question.

Command:

```bash
python3 tools/run_locomo_90_hit_rate.py \
  --threshold-profile locomo_full \
  --input /tmp/locomo10.json \
  --report /tmp/locomo_equiv_hit.json \
  --misses /tmp/locomo_equiv_hit_misses.jsonl
```

Result:

| Metric | Previous full run | After gap-fill |
| --- | ---: | ---: |
| Case count | 1,542 | 1,542 |
| Retrieval/context Hit@K | 0.9215304799 | 0.9403372244 |
| Mean reciprocal rank | 0.4882334653 | 0.5238774644 |
| Answer-term coverage | 0.6647211414 | 0.7542153048 |
| Zero-hit queries | 121 | 92 |
| Token reduction | 82.9327650730 | 82.9327650730 |
| Retrieval p95 | 105.2860259893 ms | 105.6497342361 ms |
| Threshold violations | `[]` | `[]` |

Category movement:

| Category | Previous Hit@K | After Hit@K | Previous zero-hit | After zero-hit |
| --- | ---: | ---: | ---: | ---: |
| `category_1` | 0.9255319149 | 0.9397163121 | 21 | 17 |
| `category_2` | 0.9501557632 | 0.9688473520 | 16 | 10 |
| `category_3` | 0.7812500000 | 0.8020833333 | 21 | 19 |
| `category_4` | 0.9250891795 | 0.9453032105 | 63 | 46 |

Claim level: deterministic retrieval/scoring quality improvement on the real LOCOMO artifact. This
still does not claim live OSS-reader parity because the reader mode is deterministic and
`reader_open_source_calls = 0`.

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

## LongMemEval_s Fixture Ranking Gap-Fill

Status: `fixture-mrr-improved`

This is a runner-quality validation on the checked-in fixture only. The gap-fill adds deterministic
update/current-memory ranking for questions that ask for current, latest, updated, changed, or "used
now" facts. The real HELaMem LongMemEval_s full-dataset gate is recorded in the section above.

Command:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile fixture \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --reader-mode auto \
  --report /tmp/longmemeval_fixture_after.json \
  --misses /tmp/longmemeval_fixture_after_misses.jsonl
```

Result:

| Metric | Before | After |
| --- | ---: | ---: |
| Case count | 4 | 4 |
| Hit@K | 1.0 | 1.0 |
| Reader hit rate | 1.0 | 1.0 |
| Mean reciprocal rank | 0.8333333333 | 1.0 |
| Memory-update MRR | 0.6666666667 | 1.0 |
| Threshold violations | `[]` | `[]` |

Follow-up validation on 2026-06-20 fixed the fetch helper defaults to prefer the current official
cleaned artifact and verified local-candidate install behavior:

```bash
python3 tools/fetch_longmemeval_s.py \
  --output /tmp/longmemeval_candidate_copy.json \
  --candidate-path tools/fixtures/longmemeval_s_full_path_fixture.json \
  --min-records 2
```

The candidate path was copied only after validation and reported
`status = copied_from_candidate`, `record_count = 2`, and
`sha256 = 1032b635bf8cd592f4442df1ed65b981eef0274c99c5fb2b4028c5698a9b3588`.
At that point, the real cleaned Hugging Face artifact did not complete locally before the command
timeout. A later validation used the mounted HELaMem export recorded in the full-dataset section.

The LongMemEval fixture gate was rerun after the fetcher fix:

```bash
python3 tools/run_longmemeval_s_full_path.py \
  --threshold-profile fixture \
  --input tools/fixtures/longmemeval_s_full_path_fixture.json \
  --report /tmp/longmemeval_fixture_rerun.json \
  --misses /tmp/longmemeval_fixture_rerun_misses.jsonl
```

Rerun result: `case_count = 4`, `benchmark_hit_at_k = 1.0`,
`reader_hit_rate = 1.0`, `benchmark_mean_reciprocal_rank = 1.0`,
`benchmark_threshold_passed = true`, and `benchmark_threshold_violations = []`.

Claim level: fixture ranking improvement only. This is not a real LongMemEval_s benchmark score and
does not claim production parity.

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
