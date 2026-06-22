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
OUT_DIR=/root/src/github-services/TemporalStore/output-ubuntu22/release \
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

## Still Missing For Paper-Level Parity

- Official LOCOMO file must be placed locally and validated with `--validate-dataset-only`.
- Official LongMemEval_s file must be placed locally and validated with `--validate-dataset-only`.
- Full runs must be executed without `--question-limit`.
- OSS or OpenAI-compatible reader/judge should be used for paper-style answer quality.
- MatrixArk scores must remain separate from VikingMem paper numbers until dataset, reader, judge, prompt, and scoring protocol match.
