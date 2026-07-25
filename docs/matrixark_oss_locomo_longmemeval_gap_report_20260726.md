# MatrixArk OSS LoCoMo / LongMemEval Gap Report - 2026-07-26

## Goal

Use local OSS reader models to benchmark MatrixArk/TemporalStore on LoCoMo and
LongMemEval_s, compare against OpenViking/VikingMem where runnable, and keep the
gap-closure loop active until quality, token savings, and baseline comparison
are credible.

## Local OSS Reader

- Provider: local OpenAI-compatible Hugging Face endpoint.
- Endpoint: `http://127.0.0.1:18086/v1`.
- Model: `deepset/minilm-uncased-squad2`.
- Task: extractive question answering.
- Runtime status: endpoint responded to `/v1/models` during this run.

This reader is useful for deterministic OSS smoke and medium-slice validation.
It is not a strong final reader for paper-level LoCoMo or LongMemEval claims.

## Code Fixes In This Loop

- `tools/openai_compatible_hf_reader.py` already supports extractive QA models.
- `tools/convert_locomo_to_context_jsonl.py` now expands LoCoMo evidence-window
  refs into the exact source IDs inside the allowed window.
- `tools/run_locomo_ingest_once.py` now uses the same expanded refs for Python
  scoring and passes `--evidence-window` into the Rust TemporalStore conversion.
- Dialog refs such as `D1:3` are now matched as exact `D<session>:<turn>` refs,
  preventing false matches like `D13`.

## MatrixArk Results

### LoCoMo Tiny Diagnostic

- Input: `/root/matrixark_benchmarks/data/locomo_tiny_1conv.json`.
- Cases: 2.
- Evidence window: 2.
- Retrieval Hit@K: 1.0.
- Reader hit rate: 1.0.
- Rust TemporalStore backend ready: true.
- Rust context-event ingest ready: true.
- Rust/Python retrieval parity: true.
- Token reduction: 0.0%.

The 0.0% token reduction is expected for this diagnostic because the source
universe is pre-narrowed to the gold evidence window. Do not use this run for
token-savings claims.

### LoCoMo 3-Conversation Diagnostic

- Input: `/tmp/matrixark_locomo_3conv.json`.
- Cases: 387.
- Evidence window: 2.
- Retrieval Hit@K: 1.0.
- Reader hit rate: 0.3875968992248062.
- Reader answer coverage: 0.3801452784503632.
- Token reduction: 33.54045063704262%.
- Retrieval p95: 1.4726653002071535 ms.
- Reader p95: 228.03831559995155 ms.
- Rust TemporalStore backend ready: true.
- Rust context-event ingest ready: true.
- Zero retrieval misses: 0.

This proves the corrected evidence-window scoring and Rust ingestion/retrieval
path on a medium LoCoMo slice. The main gap is reader quality: MiniLM often
extracts partial or relative spans where the benchmark expects normalized
answers.

### LongMemEval_s 50-Record Slice

- Input: `/tmp/matrixark_longmemeval_s_50.json`.
- Cases: 50.
- Retrieval Hit@K: 0.98.
- Reader hit rate: 0.56.
- Reader answer coverage: 0.5384615384615384.
- Token reduction: 98.59427979827794%.
- Retrieval p95: 250.0640535997263 ms.
- Reader p95: 305.89553734971537 ms.
- Rust TemporalStore backend ready: true.
- Rust context-event ingest ready: true.
- Zero retrieval misses: 1.

This is the strongest current token-savings result: MatrixArk retrieves compact
context with very high savings, while quality is limited by the small local QA
reader.

## OpenViking / VikingMem Baseline Status

Local OpenViking source exists at `/root/src/github-services/OpenViking`.
The benchmark READMEs require:

- running OpenViking server;
- importing each dataset into OpenViking;
- running retrieval/eval;
- running judge/stat scripts.

That local apples-to-apples baseline has not been completed in this loop.
Published OpenViking README claims are useful orientation only, not local
MatrixArk-vs-OpenViking evidence. The local baseline must be run before making
external comparison claims.

## Gap Closure Plan

1. Replace MiniLM with a stronger OSS reader for final comparison:
   Qwen 7B/14B via Ollama or vLLM, same token budget, same reader prompt, same
   scoring profile.
2. Run full LoCoMo, not evidence-window-only diagnostics:
   1,542 questions, all conversations, full source universe, explicit token
   budgets.
3. Run full LongMemEval_s:
   500 records, same max events and ContextPack budget for MatrixArk and
   OpenViking.
4. Start and configure local OpenViking, then run its import/eval/judge scripts
   against the same LoCoMo and LongMemEval files.
5. Report each system with:
   retrieval Hit@K, reader/judge accuracy, token input, token savings, p50/p95,
   reader model, embedding model, source budget, and blockers.
6. Close current MatrixArk gaps:
   improve LoCoMo answer normalization for relative dates/spans; increase reader
   strength; test retrieval without gold evidence-window narrowing; investigate
   the single LongMemEval retrieval miss.

## Current Status

- MatrixArk OSS medium-slice evidence: present.
- MatrixArk token-savings evidence: strong on LongMemEval_s 50-record slice.
- MatrixArk reader quality: not competitive enough with MiniLM.
- OpenViking/VikingMem local comparison: blocked until server/import/eval is
  run locally.
- Goal status: active, not complete.
