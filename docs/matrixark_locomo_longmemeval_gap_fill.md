# MatrixArk LOCOMO And LongMemEval Gap Fill

Date: 2026-06-22

## What Was Filled

The benchmark runner now handles more realistic dataset inputs before a long C++ TemporalStore run starts.

Closed gaps:

- JSON arrays are supported.
- JSONL files are supported.
- Wrapped JSON objects are supported, for example `{"data": [...]}`, `{"test": [...]}`, or `{"validation": [...]}`.
- LOCOMO loader accepts common field variants:
  - `conversation`, `conversations`, `sessions`, `dialogue`, `dialogs`
  - `qa`, `qas`, `questions`, `question_answer`, `question_answers`
  - turn text from `text`, `content`, `message`, `utterance`, or `value`
- LongMemEval loader accepts common field variants:
  - `haystack_sessions`, `sessions`, `conversation_sessions`, `haystack`
  - `haystack_dates` or `session_dates`
  - `haystack_session_ids` or `session_ids`
  - question from `question`, `query`, or `instruction`
  - answer from `answer`, `answers`, or `target`
  - evidence from `answer_session_ids`, `evidence_session_ids`, or `evidence`
- Runner default metaserver now matches the local C++ launcher: `127.0.0.1:18000`.
- `--validate-dataset-only` validates shape and prints counts without starting C++ TemporalStore.
- Reader and judge providers are explicit:
  - deterministic CI/debug mode by default
  - OpenAI-compatible reader/judge mode for paper-style runs
  - OpenAI-compatible mode fails fast if the configured API key env var is missing

## Validation-Only Command

```bash
PYTHONPATH=. python3 tools/run_matrixark_dataset_benchmark.py \
  --dataset locomo \
  --data-path /path/to/locomo.json \
  --artifact-dir /tmp/matrixark-bench \
  --artifact-prefix locomo-check \
  --validate-dataset-only
```

Expected output shape:

```json
{
  "dataset": "locomo",
  "items": 10,
  "conversations": 10,
  "sessions": 100,
  "turns": 10000,
  "questions": 1986,
  "missing_session_rows": 0,
  "missing_question_rows": 0,
  "status": "ok"
}
```

## C++ TemporalStore Benchmark Command

Start C++ TemporalStore:

```bash
OUT_DIR=<repo>/output-ubuntu22/release \
  bash tools/deploy_local_ubuntu22.sh start
```

Run LOCOMO:

```bash
PYTHONPATH=. python3 tools/run_matrixark_dataset_benchmark.py \
  --dataset locomo \
  --data-path /path/to/locomo.json \
  --artifact-dir /tmp/matrixark-bench/locomo \
  --artifact-prefix locomo-cpp \
  --batch-size 20 \
  --max-context-tokens 1200 \
  --temporalstore-lib output-ubuntu22/release/sdk/lib/libbcache2.so
```

Run LOCOMO with an OpenAI-compatible reader and judge:

```bash
export OPENAI_API_KEY=...

PYTHONPATH=. python3 tools/run_matrixark_dataset_benchmark.py \
  --dataset locomo \
  --data-path /path/to/locomo.json \
  --artifact-dir /tmp/matrixark-bench/locomo-gpt4o-mini \
  --artifact-prefix locomo-cpp-gpt4o-mini \
  --batch-size 20 \
  --max-context-tokens 1200 \
  --temporalstore-lib output-ubuntu22/release/sdk/lib/libbcache2.so \
  --reader-provider openai-compatible \
  --judge-provider openai-compatible \
  --reader-model gpt-4o-mini \
  --judge-model gpt-4o-mini \
  --openai-base-url https://api.openai.com/v1 \
  --openai-api-key-env OPENAI_API_KEY
```

Run LongMemEval_s:

```bash
PYTHONPATH=. python3 tools/run_matrixark_dataset_benchmark.py \
  --dataset longmemeval_s \
  --data-path /path/to/longmemeval_s.jsonl \
  --artifact-dir /tmp/matrixark-bench/longmemeval_s \
  --artifact-prefix longmemeval-cpp \
  --batch-size 20 \
  --max-context-tokens 1200 \
  --temporalstore-lib output-ubuntu22/release/sdk/lib/libbcache2.so
```

Each run writes:

- `result.json`
- `report.json`
- `report.md`
- `hypotheses.jsonl`
- `context_packs.jsonl`
- `judge.jsonl`
- `progress.json`

## Smoke Result

A wrapped LOCOMO-style sample was run through C++ TemporalStore with the new loader.

Result:

```json
{
  "dataset": "locomo",
  "questions_run": 1,
  "turns_ingested": 2,
  "context_recall": 1.0,
  "answer_hit": 1.0,
  "final_judge_score": 1.0,
  "artifacts_written": 7
}
```

This proves the flexible loader, C++ TemporalStore direct backend, batch extraction, retrieval, and canonical artifact writing work together.

## Reader And Judge Modes

Deterministic mode is for CI and regression tests. It reports:

- exact answer hit
- key-token support score
- context recall
- answer-bearing token density
- token efficiency

OpenAI-compatible mode is for paper-style answer quality. It calls `/chat/completions` on the configured base URL. The same storage, extraction, retrieval, packing, and artifact pipeline is used; only the final reader/judge changes.

Important rule: do not compare deterministic scores directly against VikingMem paper scores. Use OpenAI-compatible reader/judge when the goal is paper-style parity.

## Still Missing For Paper-Level Parity

- Official LOCOMO file must be placed locally and validated with `--validate-dataset-only`.
- Official LongMemEval_s file must be placed locally and validated with `--validate-dataset-only`.
- Full runs must be executed without `--question-limit`.
- OSS reader is still a future provider; OpenAI-compatible reader/judge is now wired.
- MatrixArk scores must remain separate from VikingMem paper numbers until dataset, reader, judge, prompt, and scoring protocol match.
