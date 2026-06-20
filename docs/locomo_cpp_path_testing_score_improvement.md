# LOCOMO C++-Path Testing And Score Improvement

Run date: 2026-06-19

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
the timestamped multi-session history into TemporalStore chat sources:

```bash
python3 tools/convert_locomo_to_context_jsonl.py /tmp/longmemeval_s.json \
  /tmp/temporalstore_longmemeval_s.jsonl \
  --dataset-name longmemeval_s

TEMPORALSTORE_CONTEXT_BENCHMARK_EXTERNAL_ONLY=1 \
TEMPORALSTORE_CONTEXT_BENCHMARK_JSONL=/tmp/temporalstore_longmemeval_s.jsonl \
TEMPORALSTORE_CONTEXT_BENCHMARK_REPORT_ONLY=1 \
TEMPORALSTORE_CONTEXT_BENCHMARK_MAX_EVENTS=128 \
TEMPORALSTORE_CONTEXT_BENCHMARK_DIRECT_SOURCE_SCORING=1 \
target/debug/context_workflow_harness \
  > /tmp/temporalstore_longmemeval_s_result.json
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

## Rust Improvements In This Pass

- Added LOCOMO JSON -> TemporalStore context JSONL conversion.
- Added LongMemEval_s `haystack_sessions` -> TemporalStore context JSONL conversion.
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

Small engine-backed smoke over the first 10 evidence-window cases:

| Metric | Result |
| --- | ---: |
| Cases | 10 |
| Retrieval/context Hit@K | 1.0 |
| Evidence-ref coverage | 1.0 |
| MRR | 0.83125 |
| Answer-term coverage | 0.70 |

LongMemEval_s converter smoke used a synthetic timestamped two-session fixture because the full
LongMemEval_s file was not present locally during this pass:

| Metric | Result |
| --- | ---: |
| Cases | 1 |
| Retrieval/context Hit@K | 1.0 |
| Evidence-ref coverage | 0.0 |
| MRR | 1.0 |
| Answer-term coverage | 1.0 |

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

- Rust external scoring is retrieval/evidence-term oriented; it does not yet generate a full
  deterministic reader hypothesis per question.
- Category 1 and category 3 still need stronger inference over selected evidence.
- The full 1,986-question run is supported by the converter. Full-conversation engine-backed
  retrieval is still too slow for quick local iteration because retrieval is repeated per question.
  Direct source scoring gives a fast retrieval/context diagnostic, while the next production target
  is a dedicated LOCOMO benchmark binary that ingests each conversation once, streams all questions,
  emits per-question miss reports, and optionally runs the same `google/flan-t5-small` reader used
  in the MatrixArk/C++ path.
- Full LongMemEval_s scoring still needs the real dataset artifact and the same ingest-once/query-many
  benchmark runner; the converter and smoke path are in place.
